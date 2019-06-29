package hy.flink.hbase;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sinks.CsvTableSink;
import org.apache.flink.types.Row;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

public class HBaseTableMain {

  public static void main(String[] args) throws Exception {

    // Create table environment.
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    BatchTableEnvironment batchTableEnvironment = BatchTableEnvironment.create(env);

    // Register HbaseTableSource.
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", "localhost");

    HBaseTableSource hBaseTableSource = new HBaseTableSource(conf, "table_pet");
    hBaseTableSource.addColumn("base_info", "age", String.class);
    hBaseTableSource.addColumn("host_info", "name", String.class);
    hBaseTableSource.addColumn("host_info", "address", String.class);

    batchTableEnvironment.registerTableSource("table_pet", hBaseTableSource);

    // Select.
    Table tapiResult = batchTableEnvironment.scan("table_pet").select("row_key, host_info");
    DataSet<Row> result = batchTableEnvironment.toDataSet(tapiResult, Row.class);
    result.print();

    // Create csv table sink.
    CsvTableSink sink = new CsvTableSink("file:///home/hy/Desktop/out.csv", "|");

    // Register the TableSink with a specific schema.
    String[] fieldNames = {"row_key", "host_info"};
    TypeInformation[] fieldTypes = tapiResult.getSchema().getFieldTypes();  // must use TypeInformations from schema.
    batchTableEnvironment.registerTableSink("CsvSinkTable", fieldNames, fieldTypes, sink);

    // Sink data.
    tapiResult.insertInto("CsvSinkTable");

    JobExecutionResult re = env.execute("");

    System.out.println("Ok");
  }

}
