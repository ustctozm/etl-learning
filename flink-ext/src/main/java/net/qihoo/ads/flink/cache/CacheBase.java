package net.qihoo.ads.flink.cache;

import org.apache.flink.api.common.functions.Function;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;


public abstract class CacheBase <K, IN, ACC, OUT, F extends Function> extends
        AbstractUdfStreamOperator<OUT, F>
        implements OneInputStreamOperator<IN, OUT> {
  transient private long currentCheckpointId;
  transient private Meter cacheAggInputCount;
  transient private Meter cacheAggOutputCount;
  transient private Gauge cacheSizeInfo;
  transient private Gauge evictionCountInfo;
  transient private Gauge cacheMaxSizeInfo;
  transient private Gauge cacheExpiredInfo;
  transient private Gauge cacheExplicitInfo;

  transient private JobCache<K, ACC, OUT> cacheStateThisJob;
  private final int evictBufferSizeLimit;
  private final int sendEvictBufferCountWhenFull;
  private final long cacheSize;
  private final long cacheEvictTime;
  private final boolean isSharedInTask;


  public CacheBase(F userFunction, int evictBufferSizeLimit, int sendEvictBufferCountWhenFull, long cacheSize, long cacheEvictTime, boolean isSharedInTask) {
    super(userFunction);
    this.evictBufferSizeLimit = evictBufferSizeLimit;
    this.sendEvictBufferCountWhenFull = sendEvictBufferCountWhenFull;
    this.cacheSize = cacheSize;
    this.cacheEvictTime = cacheEvictTime;
    this.isSharedInTask = isSharedInTask;
  }

  @Override
  public void open() throws Exception {
    super.open();

    this.currentCheckpointId = 0;
    // get configurations
    if (this.isSharedInTask) {
      String operatorName = getRuntimeContext().getJobId() + getRuntimeContext().getTaskName();
      //if using more than two static cache in one TM, we need to recognize different cache by using operatorName.
      //So we need to assign different operatorName manually in Flink Job.
      this.cacheStateThisJob = JobCache.getSharedCacheInstance(cacheSize, cacheEvictTime, operatorName);
    }
    else {
      this.cacheStateThisJob = JobCache.getCacheInstance(cacheSize, cacheEvictTime);
    }

    this.cacheAggInputCount = getRuntimeContext().getMetricGroup()
            .meter("cacheAggInputCount", new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
    this.cacheAggOutputCount = getRuntimeContext().getMetricGroup()
            .meter("cacheAggOutputCount", new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
    this.cacheSizeInfo =  getRuntimeContext().getMetricGroup().gauge("caffienCacheSize",
            (Gauge<Long>) () -> cacheStateThisJob.getCacheInfo(currentCheckpointId).getCacheSize());
    this.evictionCountInfo = getRuntimeContext().getMetricGroup().gauge("caffeineEvictionCount",
            (Gauge<Long>) () -> cacheStateThisJob.getCacheInfo(currentCheckpointId).getCacheStats().evictionCount());
    this.cacheMaxSizeInfo = getRuntimeContext().getMetricGroup().gauge("caffeineMaxSizeCount",
            (Gauge<Integer>) () -> cacheStateThisJob.getCacheInfo(currentCheckpointId).getCacheEvictInfo().getOrDefault("size", new AtomicInteger()).intValue());
    this.cacheExpiredInfo = getRuntimeContext().getMetricGroup().gauge("caffeineExpiredCount",
            (Gauge<Integer>) () -> cacheStateThisJob.getCacheInfo(currentCheckpointId).getCacheEvictInfo().getOrDefault("expired", new AtomicInteger()).intValue());
    this.cacheExplicitInfo = getRuntimeContext().getMetricGroup().gauge("caffeineExplicitCount",
            (Gauge<Integer>) () -> cacheStateThisJob.getCacheInfo(currentCheckpointId).getCacheEvictInfo().getOrDefault("explicit", new AtomicInteger()).intValue());
  }

  //输出队列信息
  protected void clearItemsInEvictBuffer(ConcurrentLinkedQueue<OUT> evictBuffer, int evictSize) {
    int evictCount = 0;
    StreamRecord<OUT> streamRecord = new StreamRecord<>(null);

    while (evictCount < evictSize && streamRecord.replace(evictBuffer.poll()).getValue() != null) {
      this.output.collect(streamRecord);
      evictCount += 1;
    }
    this.cacheAggOutputCount.markEvent(evictCount);
  }

  protected void cleanUpAllState() {
    // clear current cache, the state for this checkpointId must be empty
    this.cacheStateThisJob.cacheContentToEvictBuffer(this.currentCheckpointId);
    ConcurrentLinkedQueue<OUT> evictBuffer = this.cacheStateThisJob.getEvictBuffer(this.currentCheckpointId);
    clearItemsInEvictBuffer(evictBuffer, evictBuffer.size());
  }

  @Override
  public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
    super.prepareSnapshotPreBarrier(checkpointId);

    cleanUpAllState();
    // we dependence on the Flink checkpoint mechanism that next checkpoint is always +1 of current cp id.
    // The lifetime of cp state is longer than the lifetime of cp.
    // CP start ------> CP prepare --------> CP Finish ----> notifyCompleted ----> ...
    //          create next cp state here                 delete this cp state here
    this.currentCheckpointId = checkpointId + 1;
  }

  // From Flink 1.15, replace old.close() to new.finish(), Both can emit remaining records!
  // This method is expected to flush all remaining buffered data
  @Override
  public void finish() throws Exception {
    cleanUpAllState();
    super.finish();
  }

  @Override
  public void notifyCheckpointComplete(long checkpointId) throws Exception {
    this.cacheStateThisJob.removeCache(checkpointId);
    super.notifyCheckpointComplete(checkpointId);
  }

  // From Flink 1.15, replace dispose() to close(), Both can not emit any records!
  // This method is expected to release all resources that the operator has acquired.
  @Override
  public void close() throws Exception {
    // clear strong references
    this.cacheStateThisJob = null;
    super.close();

  }


  @Override
  public void processElement(StreamRecord<IN> element) throws Exception {
    this.cacheAggInputCount.markEvent(1L);
  }

  protected JobCache<K, ACC, OUT> getJobCacheBuffer() {
    return this.cacheStateThisJob;
  }

  protected long getCurrentCheckpointId() {
    return this.currentCheckpointId;
  }

  protected void spillEvictBuffer() {
    // output at least one item plus
    ConcurrentLinkedQueue<OUT> evictBuffer = getJobCacheBuffer().getEvictBuffer(getCurrentCheckpointId());
    int evictSize = (evictBuffer.size() > this.evictBufferSizeLimit) ? this.sendEvictBufferCountWhenFull: 1;

    clearItemsInEvictBuffer(evictBuffer, evictSize);
  }
}