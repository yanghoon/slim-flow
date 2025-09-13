package slim.flow.flink.v1.source.web;

import java.util.List;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import lombok.Builder;
import slim.flow.flink.v1.source.common.enumerator.FixedSplitEnumerator;
import slim.flow.flink.v1.source.common.serializer.SimpleListSerializer;

@Builder
public class WebSource<T> implements Source<T, WebSplit, List<WebSplit>> {

    private final List<WebSplit> splits;

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<T, WebSplit> createReader(SourceReaderContext readerContext) throws Exception {
        return new WebSourceReader<>(readerContext);
    }

    @Override
    public SplitEnumerator<WebSplit, List<WebSplit>> createEnumerator(SplitEnumeratorContext<WebSplit> enumContext)
            throws Exception {
        return FixedSplitEnumerator.<WebSplit>builder()
                .enumContext(enumContext)
                .splits(splits)
                .build();
    }

    @Override
    public SplitEnumerator<WebSplit, List<WebSplit>> restoreEnumerator(SplitEnumeratorContext<WebSplit> enumContext,
            List<WebSplit> checkpoint) throws Exception {
        return FixedSplitEnumerator.<WebSplit>builder()
                .enumContext(enumContext)
                .splits(checkpoint)
                .build();
    }

    @Override
    public SimpleVersionedSerializer<WebSplit> getSplitSerializer() {
        return new WebSplit.Serializer();
    }

    @Override
    public SimpleVersionedSerializer<List<WebSplit>> getEnumeratorCheckpointSerializer() {
        return new SimpleListSerializer<>(new WebSplit.Serializer());
    }
    
}
