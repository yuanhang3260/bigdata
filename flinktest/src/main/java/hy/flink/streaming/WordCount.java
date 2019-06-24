package hy.flink.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

public class WordCount {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//    DataStream<Tuple2<String, Integer>> dataStream = env
////        .setParallelism(2)
//        .socketTextStream("localhost", 9999)
//        .flatMap(new Splitter());
////        .keyBy(0)
////        .timeWindow(Time.seconds(10))
////        .sum(1);
//
//    dataStream.print();
//
//    JobExecutionResult result = env.execute("Window WordCount");
//    System.out.println(result.getAllAccumulatorResults());
//    System.out.println(result.getJobID());

    // *************************** Table ******************************** //
    StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.createLocalEnvironment();
    ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();

    CsvTableSource csvtable = CsvTableSource.builder()
        .path("/home/hy/Desktop/a.csv")
        .ignoreFirstLine()
        .fieldDelimiter(",")
        .field("id", Types.INT())
        .field("name", Types.STRING())
        .field("designation", Types.STRING())
        .field("age", Types.INT())
        .field("location", Types.STRING())
        .build();

    BatchTableEnvironment batchTableEnvironment = BatchTableEnvironment.create(batchEnv);
    StreamTableEnvironment streamTableEnvironment = StreamTableEnvironment.create(streamEnv);

    batchTableEnvironment.registerTableSource("table1", csvtable);

    TypeInformation<Row> type = csvtable.getReturnType();

    System.out.println("exit");
  }

  public static class Splitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
    @Override
    public void flatMap(String sentence, Collector<Tuple2<String, Integer>> out) throws Exception {
      for (String word: sentence.split(" ")) {
        if (word.isEmpty()) {
          continue;
        }
        out.collect(new Tuple2<String, Integer>(word, 1));
      }
    }
  }

}
