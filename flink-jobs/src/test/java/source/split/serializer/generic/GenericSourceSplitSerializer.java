package source.split.serializer.generic;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.util.function.SerializableFunction;

public class GenericSourceSplitSerializer<T> implements SimpleVersionedSerializer<GenericSourceSplit<T>> {

    private static final int VERSION = 1;

    // T 타입 직렬화·역직렬화용
    private final TypeSerializer<T> payloadSerializer;

    // 서브클래스 팩토리 (클래스명 → 인스턴스 생성)
    public interface SplitFactory<T> {
        GenericSourceSplit<T> create(
            String className,
            String splitId,
            SerializableFunction<String,T> processor,
            T payload,
            DataInputView in) throws IOException;
    }
    private final SplitFactory<T> factory;

    public GenericSourceSplitSerializer(
            TypeSerializer<T> payloadSerializer,
            SplitFactory<T> factory) {
        this.payloadSerializer = payloadSerializer;
        this.factory = factory;
    }

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(GenericSourceSplit<T> split) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputViewStreamWrapper out = new DataOutputViewStreamWrapper(baos);
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {

            // 1) 버전
            out.writeInt(VERSION);
            // 2) 클래스명 저장 (FQCN)
            String className = split.getClass().getName();
            out.writeUTF(className);
            // 3) splitId
            out.writeUTF(split.splitId());
            // 4) 람다 함수 (Java Serialization)
            oos.writeObject(split.getProcessor());
            oos.flush();
            // 5) payload (TypeSerializer 사용)
            payloadSerializer.serialize(split.getPayload(), out);

            // 6) 서브클래스 추가 필드 처리
            if (split instanceof MySourceSplit) {
                int extra = ((MySourceSplit<T>) split).getExtraField();
                out.writeInt(extra);
            }

            return baos.toByteArray();
        }
    }

    @Override
    public GenericSourceSplit<T> deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException("Unsupported version: " + version);
        }
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
             DataInputViewStreamWrapper in = new DataInputViewStreamWrapper(bais);
             ObjectInputStream ois = new ObjectInputStream(bais)) {

            // 1) 버전
            int version0 = in.readInt();
            // 1) 클래스명
            String className = in.readUTF();
            // 2) splitId
            String splitId = in.readUTF();
            // 3) 람다 함수 복원
            @SuppressWarnings("unchecked")
            SerializableFunction<String,T> processor =
                (SerializableFunction<String,T>) ois.readObject();
            // 4) payload 복원
            T payload = payloadSerializer.deserialize(in);

            // 5) 팩토리 호출로 서브클래스 생성(추가 필드 처리 포함)
            return factory.create(className, splitId, processor, payload, in);
        } catch (ClassNotFoundException e) {
            throw new IOException("Failed to deserialize function", e);
        }
    }

}
