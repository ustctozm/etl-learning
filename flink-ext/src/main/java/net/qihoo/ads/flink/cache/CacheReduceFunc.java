package net.qihoo.ads.flink.cache;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Meter;


public abstract class CacheReduceFunc <OUT> extends AbstractRichFunction implements ReduceFunction<OUT> {
  protected transient Meter cacheReduceMergeCount;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    cacheReduceMergeCount = getRuntimeContext().getMetricGroup().meter("cacheAggMergeCount",
            new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
  }
}
