package slim.flow.flink.v1.job.adapter.in.parser;

import java.util.List;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class PostParser {

    private static final ObjectMapper mapper = new ObjectMapper();
    private static final TypeReference<List<Post>> type = new TypeReference<List<Post>>() {};
    
    public List<Post> parse(String json) throws Exception {
        return mapper.readValue(json, type);
    }

    public static record Post(
        String title,
        String content
    ) {}

}
