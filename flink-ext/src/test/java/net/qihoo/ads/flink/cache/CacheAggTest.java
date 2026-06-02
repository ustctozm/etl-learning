package net.qihoo.ads.flink.cache;

import java.util.*;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.Test;


class TestSource extends RichParallelSourceFunction<Tuple2<Long, Long>> {
  private volatile boolean isRunning = true;
  private long i = 0L;

  @Override
  public void run(SourceContext<Tuple2<Long, Long>> ctx) {
    while (isRunning && i <= 5L) {
      ctx.collect(Tuple2.of(i , i));
      i += 1;
    }
  }

  @Override
  public void cancel() {
    isRunning = false;
  }
}

class TestAggFunc extends CacheAggFunc<Tuple2<Long, Long>, List<Long>, Long> {

  @Override
  public List<Long> createAccumulator() {
    List<Long> xs = new ArrayList<>();
    xs.add(0L);
    xs.add(0L);
    return xs;
  }

  @Override
  public List<Long> add(Tuple2<Long, Long> value, List<Long> accumulator) {
    accumulator.set(0, accumulator.get(0) + value.f0);
    accumulator.set(1, accumulator.get(1) + value.f1);
    return accumulator;
  }

  @Override
  public Long getResult(List<Long> accumulator) {
    return accumulator.get(0);
  }

  @Override
  public List<Long> merge(List<Long> a, List<Long> b) {
    List<Long> res = new ArrayList<>();
    res.add(a.get(0) + b.get(0));
    res.add(a.get(1) + b.get(1));
    return res;
  }
}

public class CacheAggTest {

  private void doTest(int curTimes, int totalTimes) throws Exception {
    if (curTimes <= totalTimes) {
      System.out.println("test time " + curTimes + "/" + totalTimes);

      StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

      env.getCheckpointConfig().setCheckpointInterval(100);
      env.getConfig().setAutoWatermarkInterval(50L);

      DataStream<Long> data = env.addSource(new TestSource()).setParallelism(2)
        .transform("cacheAggregator",
                BasicTypeInfo.LONG_TYPE_INFO,
                new CacheAggregator<>(new TestAggFunc(), v -> v.f0, 100, 10,100,3000, false)).setParallelism(2)
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

      Assert.assertEquals(30, sum);

      doTest(curTimes + 1,  totalTimes);
    }
  }

  @Test
  public void testFilter() throws Exception {
    int totalTimes = 1;
    doTest(1, totalTimes);
  }

  @Test
  public void testCacheInfo(){
    JobCache<Object, Object, Object> jobCache = JobCache.getCacheInstance(1000, 3000);
    Assert.assertEquals(jobCache.getCacheInfo(0).getCacheSize(), -1);
  }
}
