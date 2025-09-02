package source.split.serializer.generic;
import java.io.Serializable;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.util.function.SerializableFunction;

public class GenericSourceSplit<T> implements SourceSplit, Serializable {

    private final String splitId;
    private final SerializableFunction<String,T> processor;
    private final T payload;

    public GenericSourceSplit(
            String splitId,
            SerializableFunction<String,T> processor,
            T payload) {
        this.splitId = splitId;
        this.processor = processor;
        this.payload = payload;
    }

    @Override
    public String splitId() {
        return splitId;
    }

    public SerializableFunction<String,T> getProcessor() {
        return processor;
    }

    public T getPayload() {
        return payload;
    }

}
