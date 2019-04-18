package hy.flink.streaming;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class WordCount {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

      DataStream<Tuple2<String, Integer>> dataStream = env
          .socketTextStream("localhost", 9999)
          .flatMap(new Splitter())
          .keyBy(0)
          .timeWindow(Time.seconds(10))
          .sum(1);

    dataStream.print();

    JobExecutionResult result = env.execute("Window WordCount");
    System.out.println(result.getAllAccumulatorResults());
    System.out.println(result.getJobID());
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
