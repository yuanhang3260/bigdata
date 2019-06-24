package hy.flink.batch;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

public class WordCount {

  public static void main(String[] args) throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

//    DataSet<String> text = env.fromElements(
//        "Who's there?",
//        "I think I hear them. Stand, ho! Who's there?");

    DataSource<String> input = env.readTextFile("input/article.txt");

    DataSet<Integer> output = input
        .flatMap(new LineSplitter())
        .filter(x -> !x.getWord().isEmpty())
        .groupBy(WC::getWord)
        .reduce((x, y) -> {
          return new WC(x.getWord(), x.getCount() + y.getCount());
        })
        .map(new RichMapFunction<WC, WC>() {
          private LongCounter counter = new LongCounter();

          @Override
          public void open(Configuration parameters) throws Exception {
            getRuntimeContext().addAccumulator("accumulator", counter);
          }

          @Override
          public WC map(WC value) throws Exception {
            counter.add(1L);
            return value;
          }
        })
        .setParallelism(2)
        .mapPartition(new MapPartitionFunction<WC, Integer>() {
          @Override
          public void mapPartition(Iterable<WC> values, Collector<Integer> out) throws Exception {
            int count = 0;
            for (WC wc : values) {
              System.out.println(wc.toString());
              count++;
            }
            System.out.println("partition size: " + count);
            out.collect(count);
          }
        });

    output.writeAsText("output/result", FileSystem.WriteMode.OVERWRITE);
    JobExecutionResult result = env.execute();
    Long count = result.getAccumulatorResult("accumulator");
    System.out.println("\ndistinct words: " + count);

  }

  @Data
  @NoArgsConstructor
  @AllArgsConstructor
  private static class WC {
    private String word;
    private Integer count;
  }

  private static class LineSplitter implements FlatMapFunction<String, WC> {
    @Override
    public void flatMap(String line, Collector<WC> out) {
      for (String word : line.split(" ")) {
        out.collect(new WC(word, 1));
      }
    }
  }

}
