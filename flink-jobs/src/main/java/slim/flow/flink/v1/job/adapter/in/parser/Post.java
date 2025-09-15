package slim.flow.flink.v1.job.adapter.in.parser;

@Gertter
@Accessors(fluent = true)
@JsonIgnoreProperties(ignoreUnknown = true)
// @JsonFormat(shape = JsonFormat.Shape.ARRAY)
@JsonAutoDetect(fieldVisibility = Visibility.ANY)
public class Post {

    private String title;
    private String content;

}
