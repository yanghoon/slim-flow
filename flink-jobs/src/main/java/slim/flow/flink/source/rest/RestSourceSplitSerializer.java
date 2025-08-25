package slim.flow.flink.source.rest;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.flink.core.io.SimpleVersionedSerializer;

/**
 * Serializer for RestSourceSplit.
 */
public class RestSourceSplitSerializer implements SimpleVersionedSerializer<RestSourceSplit> {
    
    private static final int VERSION = 1;
    
    @Override
    public int getVersion() {
        return VERSION;
    }
    
    @Override
    public byte[] serialize(RestSourceSplit split) throws IOException {
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(bos)) {
            
            oos.writeUTF(split.splitId());
            oos.writeInt(split.getPageNumber());
            oos.writeInt(split.getPageSize());
            oos.writeUTF(split.getBaseUrl());
            oos.writeLong(split.getLastProcessedTimestamp());
            
            oos.flush();
            return bos.toByteArray();
        }
    }
    
    @Override
    public RestSourceSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException("Unknown version: " + version);
        }
        
        try (ByteArrayInputStream bis = new ByteArrayInputStream(serialized);
             ObjectInputStream ois = new ObjectInputStream(bis)) {
            
            String splitId = ois.readUTF();
            int pageNumber = ois.readInt();
            int pageSize = ois.readInt();
            String baseUrl = ois.readUTF();
            long lastProcessedTimestamp = ois.readLong();
            
            return new RestSourceSplit(splitId, pageNumber, pageSize, baseUrl, lastProcessedTimestamp);
        }
    }
}
