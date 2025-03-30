package slim.flow.flink.client;

import java.util.List;
import java.util.Map;

import feign.Feign;
import feign.Param;
import feign.RequestLine;
import feign.gson.GsonDecoder;

public interface DummyRestClient {

    // https://dummyjson.com/docs/users#users-limit_skip
    public String URL = "https://dummyjson.com";
    public String GET_USER = "GET /users?skip={skip}&limit={limit}";

    @RequestLine(GET_USER)
    UsersResponse users(@Param("skip") int skip, @Param("limit") int limit);
    // List<Map<String, String>> users(@Param("skip") int skip, @Param("limit") int limit);

    public static class UsersResponse {
        public List<Map<String, Object>> users;
    }
    
    public static DummyRestClient create() {
        return Feign.builder()
                    // .logger(new Slf4jLogger())
                    // .logLevel(Level.FULL)
                    .decoder(new GsonDecoder())
                    .target(DummyRestClient.class, URL);
    }
}
