package slim.flow.flink.source.common.serializer;

import java.io.IOException;

import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.Builder;
import lombok.NoArgsConstructor;

@Builder
@NoArgsConstructor
public class JsonStringSerializer<E> implements SimpleVersionedSerializer<E> {

    private final int version = 1;
    private final StringSerializer serializer = StringSerializer.INSTANCE;

    private final ObjectMapper mapper = new ObjectMapper();
    private final TypeReference<E> type = new TypeReference<E>() {};

    @Override
    public int getVersion() {
        return version;
    }

    @Override
    public byte[] serialize(E obj) throws IOException {
        var json = mapper.writeValueAsString(obj);
        var out = new DataOutputSerializer(json.length() + 64);
        serializer.serialize(json, out);
        return out.getSharedBuffer();
        
    }

    @Override
    public E deserialize(int version, byte[] serialized) throws IOException {
        var in = new DataInputDeserializer(serialized);
        var json = serializer.deserialize(in);
        return mapper.readValue(json, type);
    }
    
}
