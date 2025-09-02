package source.split.serializer.generic;
import org.apache.flink.util.function.SerializableFunction;

public class MySourceSplit<T> extends GenericSourceSplit<T> {

    private final int extraField;

    public MySourceSplit(
            String splitId,
            SerializableFunction<String,T> processor,
            T payload,
            int extraField) {
        super(splitId, processor, payload);
        this.extraField = extraField;
    }

    public int getExtraField() {
        return extraField;
    }

}
