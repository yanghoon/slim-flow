package slim.flow.flink.source.rest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

import javax.annotation.Nullable;

import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * SplitEnumerator for RestSource that manages page-based splits.
 * This enumerator creates splits for each page and assigns them to readers.
 */
public class RestSourceEnumerator implements SplitEnumerator<RestSourceSplit, RestSourceEnumeratorState> {
    
    private static final Logger LOG = LoggerFactory.getLogger(RestSourceEnumerator.class);
    
    private final SplitEnumeratorContext<RestSourceSplit> context;
    private final String baseUrl;
    private final int pageSize;
    private final long pollingIntervalMs;
    private final boolean isBounded;
    
    private final Queue<RestSourceSplit> pendingSplits;
    private final Set<String> assignedSplits;
    private final Set<Integer> registeredReaders;
    
    private int currentPageNumber;
    private boolean noMoreSplits;
    
    public RestSourceEnumerator(
            SplitEnumeratorContext<RestSourceSplit> context,
            String baseUrl,
            int pageSize,
            long pollingIntervalMs,
            boolean isBounded,
            @Nullable RestSourceEnumeratorState state) {
        
        this.context = context;
        this.baseUrl = baseUrl;
        this.pageSize = pageSize;
        this.pollingIntervalMs = pollingIntervalMs;
        this.isBounded = isBounded;
        
        this.pendingSplits = new ConcurrentLinkedQueue<>();
        this.assignedSplits = new HashSet<>();
        this.registeredReaders = new HashSet<>();
        
        if (state != null) {
            this.currentPageNumber = state.getCurrentPageNumber();
            this.noMoreSplits = state.isNoMoreSplits();
            this.pendingSplits.addAll(state.getPendingSplits());
            this.assignedSplits.addAll(state.getAssignedSplits());
        } else {
            this.currentPageNumber = 0; // 0부터 시작
            this.noMoreSplits = false;
        }
    }
    
    @Override
    public void start() {
        LOG.info("Starting RestSourceEnumerator with baseUrl: {}, pageSize: {}", baseUrl, pageSize);
        
        // 초기 Split 생성
        if (pendingSplits.isEmpty() && !noMoreSplits) {
            createInitialSplits();
        }
        
        // 무제한 소스의 경우 주기적으로 새로운 페이지 체크
        if (!isBounded && !noMoreSplits) {
            context.callAsync(
                this::checkForNewData,
                this::handleAsyncResult,
                0L,
                pollingIntervalMs
            );
        }
    }
    
    @Override
    public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {
        if (!registeredReaders.contains(subtaskId)) {
            LOG.warn("Received split request from unregistered reader {}", subtaskId);
            return;
        }
        
        assignSplits();
    }
    
    @Override
    public void addSplitsBack(List<RestSourceSplit> splits, int subtaskId) {
        LOG.info("Adding {} splits back from subtask {}", splits.size(), subtaskId);
        
        for (RestSourceSplit split : splits) {
            assignedSplits.remove(split.splitId());
            pendingSplits.offer(split);
        }
        
        assignSplits();
    }
    
    @Override
    public void addReader(int subtaskId) {
        LOG.info("Adding reader {}", subtaskId);
        registeredReaders.add(subtaskId);
        assignSplits();
    }
    
    @Override
    public RestSourceEnumeratorState snapshotState(long checkpointId) throws Exception {
        LOG.debug("Snapshotting state for checkpoint {}", checkpointId);
        
        return new RestSourceEnumeratorState(
            currentPageNumber,
            noMoreSplits,
            new ArrayList<>(pendingSplits),
            new HashSet<>(assignedSplits)
        );
    }
    
    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (sourceEvent instanceof PageCompletedEvent) {
            PageCompletedEvent event = (PageCompletedEvent) sourceEvent;
            handlePageCompleted(event, subtaskId);
        }
    }
    
    @Override
    public void close() throws IOException {
        LOG.info("Closing RestSourceEnumerator");
    }
    
    private void createInitialSplits() {
        // 처음에 몇 개의 페이지 Split을 미리 생성
        int initialSplitsCount = Math.max(1, registeredReaders.size() * 2);
        
        for (int i = 0; i < initialSplitsCount; i++) {
            String splitId = generateSplitId(currentPageNumber);
            RestSourceSplit split = new RestSourceSplit(splitId, currentPageNumber, pageSize, baseUrl);
            pendingSplits.offer(split);
            currentPageNumber++;
        }
        
        LOG.info("Created {} initial splits", initialSplitsCount);
    }
    
    private void assignSplits() {
        Iterator<Integer> readerIterator = registeredReaders.iterator();
        
        while (!pendingSplits.isEmpty() && readerIterator.hasNext()) {
            RestSourceSplit split = pendingSplits.poll();
            int readerId = readerIterator.next();
            
            if (split != null) {
                context.assignSplit(split, readerId);
                assignedSplits.add(split.splitId());
                LOG.debug("Assigned split {} to reader {}", split.splitId(), readerId);
                
                // 순환 방식으로 다음 리더로 이동
                if (!readerIterator.hasNext()) {
                    readerIterator = registeredReaders.iterator();
                }
            }
        }
        
        // 모든 Split이 할당되었고 더 이상 Split이 없으면 완료 신호 전송
        if (pendingSplits.isEmpty() && assignedSplits.isEmpty() && (isBounded || noMoreSplits)) {
            for (int readerId : registeredReaders) {
                context.signalNoMoreSplits(readerId);
            }
        }
    }
    
    private void handlePageCompleted(PageCompletedEvent event, int subtaskId) {
        String completedSplitId = event.getSplitId();
        assignedSplits.remove(completedSplitId);
        
        boolean hasData = event.hasData();
        
        if (!isBounded && hasData) {
            // 무제한 소스이고 데이터가 있었다면 다음 페이지 Split 생성
            String nextSplitId = generateSplitId(currentPageNumber);
            RestSourceSplit nextSplit = new RestSourceSplit(nextSplitId, currentPageNumber, pageSize, baseUrl);
            pendingSplits.offer(nextSplit);
            currentPageNumber++;
        } else if (!hasData) {
            // 데이터가 없으면 더 이상 Split 생성 중지
            noMoreSplits = true;
        }
        
        assignSplits();
    }
    
    private Boolean checkForNewData() {
        // 실제 구현에서는 API를 호출하여 새로운 데이터가 있는지 확인
        // 여기서는 단순화하여 항상 true 반환
        return !noMoreSplits;
    }
    
    private void handleAsyncResult(Boolean hasNewData, Throwable throwable) {
        if (throwable != null) {
            LOG.error("Error checking for new data", throwable);
            return;
        }
        
        if (hasNewData && !noMoreSplits) {
            // 새로운 데이터가 있다면 다음 체크 스케줄링
            context.callAsync(
                this::checkForNewData,
                this::handleAsyncResult,
                pollingIntervalMs,
                pollingIntervalMs
            );
        }
    }
    
    private String generateSplitId(int pageNumber) {
        return String.format("rest-split-%d", pageNumber);
    }
}
