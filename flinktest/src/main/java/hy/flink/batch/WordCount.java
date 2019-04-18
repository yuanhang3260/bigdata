package hy.flink.batch;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.util.Collector;

public class WordCount {

  public static void main(String[] args) throws Exception {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

//    DataSet<String> text = env.fromElements(
//        "Who's there?",
//        "I think I hear them. Stand, ho! Who's there?");

    DataSource<String> input = env.readTextFile(args[0]);
    DataSet<WC> wordCounts = input
        .flatMap(new LineSplitter())
        .filter(x -> !x.getWord().isEmpty())
        .withParameters(null)
        .groupBy(WC::getWord)
        .reduce((x, y) -> {
          return new WC(x.getWord(), x.getCount() + y.getCount());
        });

    wordCounts.print();
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
