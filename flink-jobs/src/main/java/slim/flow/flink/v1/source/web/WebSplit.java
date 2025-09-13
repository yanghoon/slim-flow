package slim.flow.flink.v1.source.web;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.util.function.SerializableFunction;

import lombok.Builder;
import lombok.Getter;
import lombok.experimental.Accessors;

@Builder
// @AllArgsConstructor
@Getter
@Accessors(fluent = true)
public class WebSplit implements SourceSplit, Serializable {

    private static final AtomicLong SPLIT_ID = new AtomicLong(0);

    @Builder.Default
    private String splitId = WebSplit.class.getSimpleName() + SPLIT_ID.getAndIncrement();
    private String origin;
    private Map<String, String> headerMap;
    private Map<String, String> queryMap;
    private SerializableFunction<String, List<Map<String, Object>>> parser;
    private SerializableFunction<WebSplit, Void> next;

    @Override
    public String splitId() {
        return splitId;
    }
    
    public String getUrl() {
        var url = new StringBuilder();
        url.append(origin);
        url.append(buildQueryString());
        return url.toString();
    }
    
    private String buildQueryString() {
        if (queryMap == null || queryMap.isEmpty()) {
            return "";
        }

        var sb = new StringBuilder();
        sb.append("?");
        queryMap.forEach((k, v) -> {
            sb.append(k).append("=").append(v).append("&");
        });

        sb.deleteCharAt(sb.length() - 1);

        return sb.toString();
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
                out.writeUTF(split.origin());
                oos.writeObject(split.headerMap());
                oos.writeObject(split.queryMap());
                oos.writeObject(split.parser());
                oos.writeObject(split.next());

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
                String origin = in.readUTF();
                var headerMap = (Map<String, String>) ois.readObject();
                var queryMap = (Map<String, String>) ois.readObject();
                var parser = (SerializableFunction<String, List<Map<String, Object>>>) ois.readObject();
                var next = (SerializableFunction<WebSplit, Void>) ois.readObject();

                this.getClass().getClassLoader().loadClass(className);
                // return new WebSplit(splitId);
                return WebSplit.builder()
                        .splitId(splitId)
                        .origin(origin)
                        .headerMap(headerMap)
                        .queryMap(queryMap)
                        .parser(parser)
                        .next(next)
                        .build();
            } catch (ClassNotFoundException e) {
                // e.printStackTrace();
                throw new IOException("Failed to deserialize function", e);
            }
        }

    }

}
