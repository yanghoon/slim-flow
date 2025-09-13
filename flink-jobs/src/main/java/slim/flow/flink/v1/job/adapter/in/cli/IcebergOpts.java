package slim.flow.flink.v1.job.adapter.in.cli;

import org.apache.logging.log4j.core.tools.picocli.CommandLine;
import org.apache.logging.log4j.core.tools.picocli.CommandLine.Command;
import org.apache.logging.log4j.core.tools.picocli.CommandLine.Option;

@Command
public class IcebergOpts {
    
    @Option(
        names = {"-t", "--table"},
        description = "The Iceberg table to write to, in the format 'database.table'",
        required = true)
    private String table; 

    @Option(
        names = {"-e", "--endpoint"},
        description = "The S3 endpoint URL",
        required = true)
    private String endpoint;

    @Option(
        names = {"-a", "--access-key"},
        description = "The S3 access key",
        required = false)
    private String accessKey;

    @Option(
        names = {"-s", "--secret-key"},
        description = "The S3 secret key",
        required = false)
    private String secretKey;

    @Option(
        names = {"--iceberg-rest"},
        description = "The Iceberg REST catalog URL",
        required = false)
    private String icebergRest;

    public static IcebergOpts parse(String[] args) {
        var opts = new IcebergOpts();
        new CommandLine(opts).setUnmatchedArgumentsAllowed(true).parse(args);
        return opts;
    }

}
