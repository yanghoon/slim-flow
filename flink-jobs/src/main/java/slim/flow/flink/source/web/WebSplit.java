package slim.flow.flink.source.web;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import lombok.Builder;

@Builder
// @AllArgsConstructor
public class WebSplit implements SourceSplit, Serializable {

    private static final AtomicLong SPLIT_ID = new AtomicLong(0);

    @Builder.Default
    private String splitId = WebSplit.class.getSimpleName() + SPLIT_ID.getAndIncrement();

    @Override
    public String splitId() {
        return splitId;
    }
    
    /**
     * Serializer for {@link WebSplit}.
     */
    public static class Serializer implements SimpleVersionedSerializer<WebSplit> {

        private static final int VERSION = 1;

        @Override
        public int getVersion() {
            return VERSION;
        }

        @Override
        public byte[] serialize(WebSplit split) throws IOException {
            try (var baos = new ByteArrayOutputStream();
                var out = new DataOutputViewStreamWrapper(baos);
                var oos = new ObjectOutputStream(baos)) {

                out.writeUTF(split.getClass().getName());
                out.writeUTF(split.splitId());

                return baos.toByteArray();
            } 
        }

        @Override
        public WebSplit deserialize(int version, byte[] serialized) throws IOException {
            try (var bais = new ByteArrayInputStream(serialized);
                var in = new DataInputViewStreamWrapper(bais);
                var ois = new ObjectInputStream(bais)) {

                String className = in.readUTF();
                String splitId = in.readUTF();

                this.getClass().getClassLoader().loadClass(className);

                return new WebSplit(splitId);
            } catch (ClassNotFoundException e) {
                // e.printStackTrace();
                throw new IOException("Failed to deserialize function", e);
            }
        }

    }

}
