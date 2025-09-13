package slim.flow.flink.v1.job.adapter.in.cli;

import java.util.Map;

import org.apache.logging.log4j.core.tools.picocli.CommandLine;
import org.apache.logging.log4j.core.tools.picocli.CommandLine.Command;
import org.apache.logging.log4j.core.tools.picocli.CommandLine.Option;

import lombok.experimental.var;
import slim.flow.flink.v1.source.web.WebSplit;
import slim.flow.flink.v1.source.web.WebSplit.WebSplitBuilder;

@Command
public class WebSplitOpts {

    @Option(
        names = {"-u", "--url"},
        description = "The URL to fetch data from",
        required = true)
    private String origin;

    @Option(
        names = {"-h", "--header"},
        description = "HTTP headers to include in the request, in the format 'key=val'",
        required = false)
    private Map<String, String> header;

    @Option(
        names = {"-q", "--query"},
        description = "Query parameters to include in the request, in the format 'key=val'",
        required = false)
    private Map<String, String> query;

    public WebSplitBuilder webSplit() {
        return WebSplit.builder()
                .headerMap(header)
                .queryMap(query);
    }
    
    public static WebSplitOpts parseArgs(String[] args) {
        var cmd = new WebSplitOpts();
        new CommandLine(cmd)
            .setUnmatchedArgumentsAllowed(true)
            .parse(args);
        return cmd;
    }

}
