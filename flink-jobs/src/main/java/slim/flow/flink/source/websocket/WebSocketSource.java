package slim.flow.flink.source.websocket;

import java.util.List;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import slim.flow.flink.source.common.enumerator.FixedSplitEnumerator;
import slim.flow.flink.source.common.serializer.JsonStringSerializer;

public class WebSocketSource implements Source<String, WebSocketSplit, List<WebSocketSplit>> {

    private List<WebSocketSplit> splits = List.of();

    public WebSocketSource(WebSocketSplit... splits) {
        this.splits = List.of(splits);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator<WebSocketSplit, List<WebSocketSplit>> createEnumerator(
            SplitEnumeratorContext<WebSocketSplit> enumContext) throws Exception {
        return FixedSplitEnumerator.<WebSocketSplit>builder()
                .enumContext(enumContext)
                .build();
    }

    @Override
    public SplitEnumerator<WebSocketSplit, List<WebSocketSplit>> restoreEnumerator(
            SplitEnumeratorContext<WebSocketSplit> enumContext, List<WebSocketSplit> checkpoint) throws Exception {
        return FixedSplitEnumerator.<WebSocketSplit>builder()
                .enumContext(enumContext)
                .splits(checkpoint)
                .build();
    }

    @Override
    public SourceReader<String, WebSocketSplit> createReader(SourceReaderContext readerContext) throws Exception {
        return new WebSocketReader(readerContext);
    }

    @Override
    public SimpleVersionedSerializer<WebSocketSplit> getSplitSerializer() {
        return new JsonStringSerializer<WebSocketSplit>();
    }

    @Override
    public SimpleVersionedSerializer<List<WebSocketSplit>> getEnumeratorCheckpointSerializer() {
        return new JsonStringSerializer<List<WebSocketSplit>>();
    }

}
