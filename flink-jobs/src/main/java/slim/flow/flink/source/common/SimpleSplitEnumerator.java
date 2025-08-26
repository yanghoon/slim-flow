package slim.flow.flink.source.common;

import java.io.IOException;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;

public interface SimpleSplitEnumerator<SplitT extends SourceSplit, CheckpointT> extends SplitEnumerator<SplitT, CheckpointT> {

    @Override
    default public void start() {}

    @Override
    default public void close() throws IOException {}

    // @Override
    // public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {}

    // @Override
    // public void addSplitsBack(List<SplitT> splits, int subtaskId) {}

    @Override
    default public void addReader(int subtaskId) {}

    @Override
    default public CheckpointT snapshotState(long checkpointId) throws Exception { return null; }

}
