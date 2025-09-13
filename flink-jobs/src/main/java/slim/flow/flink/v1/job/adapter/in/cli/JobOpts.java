package slim.flow.flink.v1.job.adapter.in.cli;

import org.apache.logging.log4j.core.tools.picocli.CommandLine;
import org.apache.logging.log4j.core.tools.picocli.CommandLine.Command;

import lombok.Getter;
import lombok.experimental.Accessors;

@Command
@Getter
@Accessors(fluent = true)
public class JobOpts {

    public static JobOpts parse(String[] args) {
        var cmd = new JobOpts();
        new CommandLine(cmd)
            .setUnmatchedArgumentsAllowed(true)
            .parse(args);
        return cmd;
    }

}
