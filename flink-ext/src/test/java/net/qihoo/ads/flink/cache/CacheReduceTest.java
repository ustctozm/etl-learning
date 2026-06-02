package net.qihoo.ads.flink.cache;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


class TestReduceSource extends RichParallelSourceFunction<Long> {
  private volatile boolean isRunning = true;
  private long i = 0L;

  @Override
  public void run(SourceContext<Long> ctx) {
    while (isRunning && i <= 5L) {
      ctx.collect(i);
      i += 1;
    }
  }

  @Override
  public void cancel() {
    isRunning = false;
  }
}

class TestReduceFunc extends CacheReduceFunc<Long> {
  @Override
  public Long reduce(Long t, Long u) {
    return t + u;
  }
}

public class CacheReduceTest {

  private void doTest(int curTimes, int totalTimes) throws Exception {
    if (curTimes <= totalTimes) {
      System.out.println("test time " + curTimes + "/" + totalTimes);

      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

      env.getCheckpointConfig().setCheckpointInterval(100);
      env.getConfig().setAutoWatermarkInterval(50L);

      DataStream<Long> data = env.addSource(new TestReduceSource()).setParallelism(2)
              .transform("cacheReduce",
                      BasicTypeInfo.LONG_TYPE_INFO,
                      new CacheReducer<>(new TestReduceFunc(), v -> v,100,10,100,3000, true)).setParallelism(2)
              .keyBy(v -> 0)
              .process(new KeyedProcessFunction<Integer, Long, Long>() {
                @Override
                public void processElement(Long value, Context ctx, Collector<Long> out) throws Exception {
                  out.collect(value);
                }
              });
      Iterator<Long> outputIter = data.executeAndCollect();

      long sum = 0;
      while (outputIter.hasNext()) {
        long curVal = outputIter.next();
        sum += curVal;
      }
      Assert.assertEquals(sum,30);
      doTest(curTimes + 1,  totalTimes);
    }
  }

  @Test
  public void testFilter() throws Exception {
    int totalTimes = 1;
    doTest(1, totalTimes);
  }
}
