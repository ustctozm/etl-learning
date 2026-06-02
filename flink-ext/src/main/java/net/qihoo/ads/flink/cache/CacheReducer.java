package net.qihoo.ads.flink.cache;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Function;


public class CacheReducer <K, V> extends CacheBase<K, V, V, V, CacheReduceFunc<V>> {

  private final KeySelector<V, K> keySelector;

  public CacheReducer(CacheReduceFunc<V> userFunction, KeySelector<V, K> keySelector, int evictBufferSizeLimit, int sendEvictBufferCountWhenFull, long cacheSize, long cacheEvictTime, boolean isSharedInTask) {
    super(userFunction, evictBufferSizeLimit, sendEvictBufferCountWhenFull, cacheSize, cacheEvictTime, isSharedInTask);
    this.keySelector = keySelector;
  }

  @Override
  public void processElement(StreamRecord<V> element) throws Exception {
    // merge input element to cache
    super.processElement(element);
    ConcurrentMap<K,V> cacheAsMap = getJobCacheBuffer().getCache(getCurrentCheckpointId(),
            Function.identity());

    cacheAsMap.merge(this.keySelector.getKey(element.getValue()), element.getValue(),(v1, v2) -> {
            try {
              return this.getUserFunction().reduce(v1, v2);
            } catch (Exception ignore) {
              return v1; // default use v1
            }});
    spillEvictBuffer();
  }
}
