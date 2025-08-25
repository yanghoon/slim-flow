package slim.flow.flink.source.rest;

import org.apache.flink.api.connector.source.SourceSplit;
import java.io.Serializable;
import java.util.Objects;

/**
 * RestSourceSplit represents a page-based split for REST API calls.
 * Each split contains information about which page to fetch.
 */
public class RestSourceSplit implements SourceSplit, Serializable {
    
    private final String splitId;
    private final int pageNumber;
    private final int pageSize;
    private final String baseUrl;
    private final long lastProcessedTimestamp;
    
    public RestSourceSplit(String splitId, int pageNumber, int pageSize, String baseUrl) {
        this(splitId, pageNumber, pageSize, baseUrl, 0L);
    }
    
    public RestSourceSplit(String splitId, int pageNumber, int pageSize, String baseUrl, long lastProcessedTimestamp) {
        this.splitId = splitId;
        this.pageNumber = pageNumber;
        this.pageSize = pageSize;
        this.baseUrl = baseUrl;
        this.lastProcessedTimestamp = lastProcessedTimestamp;
    }
    
    @Override
    public String splitId() {
        return splitId;
    }
    
    public int getPageNumber() {
        return pageNumber;
    }
    
    public int getPageSize() {
        return pageSize;
    }
    
    public String getBaseUrl() {
        return baseUrl;
    }
    
    public long getLastProcessedTimestamp() {
        return lastProcessedTimestamp;
    }
    
    public RestSourceSplit withNewPage(int newPageNumber) {
        return new RestSourceSplit(splitId, newPageNumber, pageSize, baseUrl, lastProcessedTimestamp);
    }
    
    public RestSourceSplit withTimestamp(long timestamp) {
        return new RestSourceSplit(splitId, pageNumber, pageSize, baseUrl, timestamp);
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RestSourceSplit that = (RestSourceSplit) o;
        return Objects.equals(splitId, that.splitId);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(splitId);
    }
    
    @Override
    public String toString() {
        return "RestSourceSplit{" +
                "splitId='" + splitId + '\'' +
                ", pageNumber=" + pageNumber +
                ", pageSize=" + pageSize +
                ", baseUrl='" + baseUrl + '\'' +
                ", lastProcessedTimestamp=" + lastProcessedTimestamp +
                '}';
    }
}
