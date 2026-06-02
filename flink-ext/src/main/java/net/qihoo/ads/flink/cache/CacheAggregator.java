package net.qihoo.ads.flink.cache;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import java.util.concurrent.ConcurrentMap;


public class CacheAggregator<K, IN, ACC, OUT> extends CacheBase<K, IN, ACC, OUT, CacheAggFunc<IN, ACC, OUT>> {
  private final KeySelector<IN, K> keySelector;

  public CacheAggregator(CacheAggFunc<IN, ACC, OUT> userFunction, KeySelector<IN, K> keySelector, int evictBufferSizeLimit, int sendEvictBufferCountWhenFull, long cacheSize, long cacheEvictTime, boolean isSharedInTask) {
    super(userFunction, evictBufferSizeLimit, sendEvictBufferCountWhenFull, cacheSize, cacheEvictTime, isSharedInTask);
    this.keySelector = keySelector;
  }

  @Override
  public void open() throws Exception {
    super.open();
  }


  @Override
  public void processElement(StreamRecord<IN> element) throws Exception {
    // merge input element to cache
    super.processElement(element);

    ConcurrentMap<K,ACC>  cacheAsMap = getJobCacheBuffer().getCache(getCurrentCheckpointId(),
            value -> this.getUserFunction().getResult(value));
    ACC singleValueAcc = this.getUserFunction().createAccumulator();
    singleValueAcc = this.getUserFunction().add(element.getValue(), singleValueAcc);

    cacheAsMap.merge(this.keySelector.getKey(element.getValue()), singleValueAcc,
            (acc1, acc2) -> this.getUserFunction().merge(acc1, acc2));

    spillEvictBuffer();
  }
}
