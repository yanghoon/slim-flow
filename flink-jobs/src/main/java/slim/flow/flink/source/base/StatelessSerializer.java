package slim.flow.flink.source.base;

import java.io.IOException;

import org.apache.flink.core.io.SimpleVersionedSerializer;

public class StatelessSerializer implements SimpleVersionedSerializer<Object> {

    private static StatelessSerializer INSTANCE = new StatelessSerializer();

    private StatelessSerializer() {}

    public static <T> SimpleVersionedSerializer<T> create() {
        return (SimpleVersionedSerializer<T>) INSTANCE;
    }

    @Override
    public int getVersion() {
        return 1;
    }

    @Override
    public byte[] serialize(Object obj) throws IOException {
        return null;
    }

    @Override
    public String deserialize(int version, byte[] serialized) throws IOException {
        return null;
    }
    
}
