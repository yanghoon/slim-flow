package slim.flow.flink.source;

import java.io.IOException;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;

public class Stateless implements SourceSplit {
    private static Stateless INSTANCE = new Stateless();

    @Override
    public String splitId() {
        return null;
    }

    public static <T> Stateless get() {
        return Stateless.INSTANCE;
    }

    public static <T> Serializer serializer() {
        return Serializer.INSTANCE;
    }

    private static class Serializer implements SimpleVersionedSerializer<Stateless> {

        private static Serializer INSTANCE = new Serializer();

        @Override
        public int getVersion() {
            return 0;
        }

        @Override
        public byte[] serialize(Stateless obj) throws IOException {
            return null;
        }

        @Override
        public Stateless deserialize(int version, byte[] serialized) throws IOException {
            return Stateless.INSTANCE;
        }

    }
    
}
