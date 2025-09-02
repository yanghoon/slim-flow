package source.split.serializer;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.function.SerializableFunction;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import source.split.serializer.generic.GenericSourceSplit;
import source.split.serializer.generic.GenericSourceSplitSerializer;
import source.split.serializer.generic.MySourceSplit;

public class SourceSplitSerializerTests {
    
    private SimpleVersionedSerializer<GenericSourceSplit<Integer>> serializer;

    @BeforeEach
    void setUp() {
        // SplitFactory 구현: MySourceSplit와 GenericSourceSplit 구분
        GenericSourceSplitSerializer.SplitFactory<Integer> factory =
            (className, splitId, processor, payload, in) -> {
                if (className.equals(MySourceSplit.class.getName())) {
                    int extra = in.readInt();
                    return new MySourceSplit<>(splitId, processor, payload, extra);
                } else {
                    return new GenericSourceSplit<>(splitId, processor, payload);
                }
            };

        // IntSerializer 사용
        TypeSerializer<Integer> intSerializer = IntSerializer.INSTANCE;

        serializer = new GenericSourceSplitSerializer<>(intSerializer, factory);
    }

    @Test
    void testSerializeDeserialize_GenericSplit() throws IOException {
        // Arrange
        SerializableFunction<String, Integer> func = s -> s.length();
        GenericSourceSplit<Integer> original = new GenericSourceSplit<>("generic-1", func, 123);

        // Act
        byte[] bytes = serializer.serialize(original);
        GenericSourceSplit<Integer> restored = serializer.deserialize(serializer.getVersion(), bytes);

        // Assert: 클래스명
        assertEquals(GenericSourceSplit.class.getName(), restored.getClass().getName(), "복원된 클래스명이 GenericSourceSplit 이어야 합니다.");

        // Assert: splitId
        assertEquals("generic-1", restored.splitId(), "splitId가 일치해야 합니다.");

        // Assert: payload
        assertEquals(123, restored.getPayload(), "payload 값이 일치해야 합니다.");

        // Assert: processor 실행 결과
        assertEquals(5, restored.getProcessor().apply("hello"), "SerializableFunction 결과가 일치해야 합니다.");
    }

    @Test
    void testSerializeDeserialize_MySourceSplit() throws IOException {
        // Arrange
        SerializableFunction<String, Integer> func = s -> s.indexOf('a');
        MySourceSplit<Integer> original = new MySourceSplit<>("my-1", func, 999, 42);

        // Act
        byte[] bytes = serializer.serialize(original);
        GenericSourceSplit<Integer> restored = serializer.deserialize(serializer.getVersion(), bytes);

        // Assert: 클래스명
        assertEquals(MySourceSplit.class.getName(), restored.getClass().getName(), "복원된 클래스명이 MySourceSplit 이어야 합니다.");

        // Assert: splitId
        assertEquals("my-1", restored.splitId(), "splitId가 일치해야 합니다.");

        // Assert: payload
        assertEquals(999, restored.getPayload(), "payload 값이 일치해야 합니다.");

        // Assert: extraField
        assertTrue(restored instanceof MySourceSplit, "restored는 MySourceSplit 인스턴스여야 합니다.");
        MySourceSplit<Integer> restoredMy = (MySourceSplit<Integer>) restored;
        assertEquals(42, restoredMy.getExtraField(), "extraField 값이 일치해야 합니다.");

        // Assert: processor 실행 결과
        assertEquals(0, restoredMy.getProcessor().apply("abc"), "SerializableFunction 결과가 일치해야 합니다.");
    }

}
