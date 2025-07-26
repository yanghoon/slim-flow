package slim.flow.flink.source;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;

public class IntervalSource implements Source<Long, Stateless, Stateless>
// UnboundednessSource<Long, NoDataSplit, NoDataCheckPoint>
{

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SimpleVersionedSerializer<Stateless> getSplitSerializer() {
        return Stateless.serializer();
    }

    @Override
    public SimpleVersionedSerializer<Stateless> getEnumeratorCheckpointSerializer() {
        return Stateless.serializer();
    }

    @Override
    public SplitEnumerator<Stateless, Stateless> createEnumerator(
            SplitEnumeratorContext<Stateless> enumContext) throws Exception {
        return new IntervalEnumerator(enumContext);
    }

    @Override
    public SplitEnumerator<Stateless, Stateless> restoreEnumerator(
            SplitEnumeratorContext<Stateless> enumContext, Stateless checkpoint) throws Exception {
        return new IntervalEnumerator(enumContext, checkpoint);
    }
    
    @Override
    public SourceReader<Long, Stateless> createReader(SourceReaderContext readerContext) throws Exception {
        return new IntervalReader(readerContext);
    }

}
