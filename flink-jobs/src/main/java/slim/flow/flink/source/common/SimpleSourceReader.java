package slim.flow.flink.source.common;

import java.io.IOException;
import java.util.List;

import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceSplit;

public interface SimpleSourceReader<T, SplitT extends SourceSplit> extends SourceReader<T, SplitT> {

    @Override
    default void start() {}

    // @Override
    // default void addSplits(List<SplitT> splits) {}

    // @Override
    // InputStatus pollNext(ReaderOutput<T> output) throws Exception {}

    @Override
    default void close() throws IOException {}

    @Override
    default void notifyNoMoreSplits() {}

    @Override
    default List<SplitT> snapshotState(long checkpointId) { return null; }
}
