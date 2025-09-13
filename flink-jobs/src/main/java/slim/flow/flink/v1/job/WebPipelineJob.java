package slim.flow.flink.v1.job;

import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.flink.CatalogLoader;
import org.apache.iceberg.flink.TableLoader;
import org.apache.iceberg.flink.sink.FlinkSink;

import slim.flow.flink.v1.job.adapter.in.cli.IcebergOpts;
import slim.flow.flink.v1.job.adapter.in.cli.JobOpts;
import slim.flow.flink.v1.job.adapter.in.cli.WebSplitOpts;
import slim.flow.flink.v1.source.web.WebSource;

public class WebPipelineJob {
    
    public static void main(String[] args) {
        var webSplitBuilder = WebSplitOpts.parseArgs(args).webSplit();
        var icebergOpts = IcebergOpts.parse(args);
        var jobOpts = JobOpts.parse(args);

        // Configuration
        var env = StreamExecutionEnvironment.getExecutionEnvironment();
        // env.enableCheckpointing(jobOpts.checkpointInterval());

        // Source
        var split = webSplitBuilder
                        .parser(null)
                        .next(null)
                        .build();
        var source = WebSource.<Map<String,String>>builder()
                        .splits(List.of(split))
                        .build();
        
        // Transform
        var stream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "web-source");

        // Sink
        var rowDataStream = env.fromData(
            rowData(10, "admin", 39),
            rowData(20, "user1", 27),
            rowData(30, "user2", 26)
        );
        var tableLoader = tableLoader(icebergOpts);

        FlinkSink.forRowData(rowDataStream)
            .tableLoader(tableLoader)
            .append();

        // Sink
        // var rowDataStream = stream.
        stream.print();
    }

    private TableLoader tableLoader(IcebergOpts icebergOpts) {
        var flienkConfig = flinkConfig(icebergOpts);
        var catalog = CatalogLoader.rest("iceberg", new Configuration(), flienkConfig);
        return TableLoader.fromCatalog(catalog, TableIdentifier.of("default", "test"));
    }

    private Map<String, String> flinkConfig(IcebergOpts icebergOpts) {
        return Map.of();
    }

    private static RowData rowData(int id, String name, int age) {
        var rowData = new GenericRowData(3);
        rowData.setField(0, id);
        rowData.setField(1, StringData.fromString(name));
        rowData.setField(2, age);
        return rowData;
    }

}
