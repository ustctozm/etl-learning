package net.qihoo.ads.flink.kafka;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.connectors.kafka.internals.FlinkKafkaInternalProducer;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.RetriableException;

import static org.apache.flink.util.Preconditions.checkArgument;

public class RetryableProducerCallback<IN> implements Callback {
    private static final org.slf4j.Logger LOG = net.qihoo.ads.flink.utils.Loggers.get();
    private final IN value;
    private final FlinkKafkaInternalProducer<byte[], byte[]> produce;
    private int retry;
    private final FlinkKafkaPartitioner<IN> flinkKafkaPartitioner;
    private ProducerRecord<byte[], byte[]> record;
    private final TopicPartitions topicPartitions;
    private final Callback callback;
    // If healthy partition number below topicPartitions.length * defaultPartitionsHealthyFactor,
    // We should use default callback to throw exception.
    private static final float defaultPartitionsHealthyFactor = 0.3f;
    private final float partitionsHealthyFactor;
    private final Meter retryMeter;

    public RetryableProducerCallback(IN value,
                                     FlinkKafkaInternalProducer<byte[], byte[]> producer,
                                     int actualCallbackRetryTimes,
                                     FlinkKafkaPartitioner<IN> flinkKafkaPartitioner,
                                     ProducerRecord<byte[], byte[]> record,
                                     RuntimeContext context,
                                     Meter retryMeter,
                                     TopicPartitions topicPartitions,
                                     Callback callback) {
      this(value, producer, actualCallbackRetryTimes, flinkKafkaPartitioner, record, context, retryMeter,
              topicPartitions, defaultPartitionsHealthyFactor, callback);
    }
    public RetryableProducerCallback(IN value,
                                     FlinkKafkaInternalProducer<byte[], byte[]> producer,
                                     int actualCallbackRetryTimes,
                                     FlinkKafkaPartitioner<IN> flinkKafkaPartitioner,
                                     ProducerRecord<byte[], byte[]> record,
                                     RuntimeContext context,
                                     Meter retryMeter,
                                     TopicPartitions topicPartitions,
                                     float partitionsHealthyFactor,
                                     Callback callback) {
        checkArgument(producer != null, "producer cannot be null!");
        checkArgument(record != null, "record cannot be null!");
        checkArgument(topicPartitions != null, "topicPartitions cannot be null!");
        checkArgument(callback != null, "callback cannot be null!");
        checkArgument(partitionsHealthyFactor >= 0 && partitionsHealthyFactor < 1, "partitionsHealthyFactor must be [0,1))!");

        if (flinkKafkaPartitioner == null) {
            this.flinkKafkaPartitioner = new BalancedFlinkKafkaPartitioner<>();
            this.flinkKafkaPartitioner.open(context.getIndexOfThisSubtask(), context.getNumberOfParallelSubtasks());
        } else {
            this.flinkKafkaPartitioner = flinkKafkaPartitioner;
        }
        this.retryMeter = retryMeter;
        this.value = value;
        this.produce = producer;
        this.retry = actualCallbackRetryTimes;
        this.record = record;
        this.topicPartitions = topicPartitions;
        this.callback = callback;
        this.partitionsHealthyFactor = partitionsHealthyFactor;
    }


    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        // When metadata is NULL, it mostly throws follow exception. We should throw it to Main Thread immediately.
        // "Received invalid metadata error in produce request on partition due to org.apache.kafka.common.errors.NetworkException:
        // The server disconnected before a response was received"
        if (metadata == null) {
            // If exception == null too, We throw our custom exception to Main Thread.
            if (exception == null) {
                exception = new RuntimeException("Received metadata failed! metadata is NULL and exception is NULL also!");
            }
            callback.onCompletion(null, exception);
            return;
        }
        if (exception != null) {
            try {
                // get failed message's topic && partition
                String topic = metadata.topic();
                int badPartition = metadata.partition();
                topicPartitions.addBadPartition(topic, badPartition);
                int[] newPartitions = topicPartitions.getAvailablePartitions(topic);

                // If exception is not RetriableException, end retry action ahead.
                if (!(exception instanceof RetriableException)) {
                    callback.onCompletion(metadata, exception);
                    return;
                }

                if (newPartitions == null || newPartitions.length == 0 || (topicPartitions.getTopicPartitions(topic)!= null &&
                        newPartitions.length <= topicPartitions.getTopicPartitions(topic).length * partitionsHealthyFactor)) {
                    callback.onCompletion(metadata, exception);
                    return;
                }

                // Limit callback recursion depth, default three times
                if (retry-- <= 0) {
                    callback.onCompletion(metadata, exception);
                    return;
                }

                // -----try to send this message again-----
                int partition = flinkKafkaPartitioner.partition(
                        value,
                        record.key(),
                        record.value(),
                        topic,
                        newPartitions
                );
                record = new ProducerRecord<>(
                        topic,
                        partition,
                        record.timestamp(),
                        record.key(),
                        record.value(),
                        record.headers()
                );
                // retry send message, using recursion
                try {
                    produce.send(record, this);
                } catch (RetriableException e){
                    // If producer.send failed by RetriableException, retry onCompletion.
                    this.onCompletion(metadata, exception);
                } catch (Exception e) {
                    // If produce.send failed, We just return For throwing exception immediately.
                    // Meanwhile, avoiding massive log.error by ProducerBatch.
                    exception.addSuppressed(e);
                    callback.onCompletion(metadata, exception);
                    return;
                }
                retryMeter.markEvent();
                LOG.debug("retry send record, old target partition={} new partition={}, left {} times.",
                        badPartition,
                        partition,
                        retry);
            } catch (Throwable e) {
                exception.addSuppressed(e);
                // Cover all exception, Ensure that it can send exception to main Thread for throwing error immediately.
                // This action contains "metadata.topic()" "getAvailablePartitions(topic)" "flinkKafkaPartitioner.partition()"
                callback.onCompletion(metadata, exception);
            }
        } else {
            callback.onCompletion(metadata, null);
        }

    }

}
