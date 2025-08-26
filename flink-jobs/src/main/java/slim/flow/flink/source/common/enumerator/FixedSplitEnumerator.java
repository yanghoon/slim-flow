package slim.flow.flink.source.common.enumerator;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.Nullable;

import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import slim.flow.flink.source.common.SimpleSplitEnumerator;

@Slf4j
@AllArgsConstructor
@Builder
public class FixedSplitEnumerator<SplitT extends SourceSplit> implements SimpleSplitEnumerator<SplitT, List<SplitT>> {

    private SplitEnumeratorContext<SplitT> enumContext;
    private List<SplitT> splits = new ArrayList<>();

    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        if (splits.isEmpty()) {
            log.warn("No splits to assign");
            return;
        }

        enumContext.assignSplit(splits.remove(0), subtaskId);
    }

    @Override
    public void addSplitsBack(List<SplitT> splits, int subtaskId) {
        splits.addAll(splits);
    }
    
}
