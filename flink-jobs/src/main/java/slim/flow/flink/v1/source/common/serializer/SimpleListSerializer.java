package slim.flow.flink.v1.source.common.serializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import lombok.RequiredArgsConstructor;

@RequiredArgsConstructor
public class SimpleListSerializer<T> implements SimpleVersionedSerializer<List<T>> {

    private final SimpleVersionedSerializer<T> elementSerializer;

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(List<T> list) throws IOException {
        try (var baos = new ByteArrayOutputStream();
                var out = new DataOutputViewStreamWrapper(baos);
                var oos = new ObjectOutputStream(baos)) {

            out.writeInt(list.size());
            for (var element : list) {
                var bytes = elementSerializer.serialize(element);
                out.writeInt(bytes.length);
                out.write(bytes);
            }
            return baos.toByteArray();
        }
    }

    @Override
    public List<T> deserialize(int version, byte[] serialized) throws IOException {
        try (var bais = new ByteArrayInputStream(serialized);
                var in = new DataInputViewStreamWrapper(bais);
                var ois = new ObjectInputStream(bais)) {

            var size = in.readInt();
            var list = new java.util.ArrayList<T>(size);
            for (int i = 0; i < size; i++) {
                var length = in.readInt();
                var bytes = new byte[length];
                in.readFully(bytes);
                var element = elementSerializer.deserialize(elementSerializer.getVersion(), bytes);
                list.add(element);
            }
            return list;
        }
    }

}
