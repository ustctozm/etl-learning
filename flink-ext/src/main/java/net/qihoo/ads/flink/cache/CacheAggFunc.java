package net.qihoo.ads.flink.cache;

import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Meter;


public abstract class CacheAggFunc<IN, ACC, OUT> extends AbstractRichFunction implements AggregateFunction<IN, ACC, OUT> {

  protected transient Meter cacheAggMergeCount;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    cacheAggMergeCount = getRuntimeContext().getMetricGroup().meter("cacheAggMergeCount",
            new DropwizardMeterWrapper(new com.codahale.metrics.Meter()));
  }
}
