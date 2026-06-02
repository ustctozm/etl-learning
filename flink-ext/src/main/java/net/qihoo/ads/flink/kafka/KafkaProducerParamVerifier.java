package net.qihoo.ads.flink.kafka;

import java.util.Properties;

import net.qihoo.ads.patched.org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import static net.qihoo.ads.patched.org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.RETRY_TIMES_CUSTOM_CALLBACK;
import static net.qihoo.ads.patched.org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer.defaultCallbackRetryTimes;
import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * 针对kafka 和 flink 相关参数的验证，确保kafka的错误不会被flink框架限制住
 */
public class KafkaProducerParamVerifier {
    private static final org.slf4j.Logger LOG = net.qihoo.ads.flink.utils.Loggers.get();
    public final int transactionTimeout;
    public final long deliveryTimeout;
    public final long maxBlock;
    public final long linger;
    public final long cpInterval;
    public final long cpTimeout;
    public final long requestTimeout;
    public final int actualCallbackRetryTimes;
    public FlinkKafkaProducer.Semantic semantic;


    // 关于Kafka&&Flink 相关参数说明
    //     1.max.block.ms：执行send函数的最大时间，可以等同与等待数据写入Accumulator的最大时间。
    //     2.linger.ms: Accumulator主动写出动作等待时间。
    //     3.delivery.timeout.ms: send执行完成后，等待kafka broker确认超时时间，返回信息会在send设置的拦截器中callback中给出。
    //     4.transaction.timeout.ms: 执行完flush()后等待所有数据完成ack的超时时间，该动作在Flink的snapshotState中执行。
    //     所以也可以认为是和Flink的checkPointTimeout起始时间对齐。
    //     5.request.timeout.ms: 与kafka cluster通讯超时时间。
    //                             ————————————————————————————————————————————————————————————————————————
    //                           \｜/                                                                     ｜
    //                        (interceptor)                    /-----pull-------sender thread------>\     ｜
    //     |----------send-------------->[Accumulator]-------《                                     [kafka cluster]
    //                                                         \-----flush()----sender thread------>/
    //     |      1.max.block.ms         |  2.linger.ms         |
    //     |                             |  3.delivery.timeout.ms
    //                     invoke()                               snapshotState()
    //     |                                                             ｜ 4.transaction.timeout.ms
    public KafkaProducerParamVerifier(Properties kafkaProps,
                                      CheckpointConfig checkpointConfig) {
        ProducerConfig producerConfig = new ProducerConfig(kafkaProps);
        actualCallbackRetryTimes =  Integer.parseInt(kafkaProps.getProperty(RETRY_TIMES_CUSTOM_CALLBACK, defaultCallbackRetryTimes));
        checkArgument(actualCallbackRetryTimes >= 1, "actualCallbackRetryTimes cannot be a positive integer!");
        transactionTimeout = producerConfig.getInt("transaction.timeout.ms");
        deliveryTimeout = producerConfig.getInt("delivery.timeout.ms");
        maxBlock = producerConfig.getLong("max.block.ms");
        linger = producerConfig.getLong("linger.ms");
        requestTimeout = producerConfig.getInt("request.timeout.ms");
        cpInterval = checkpointConfig.getCheckpointInterval();
        cpTimeout = checkpointConfig.getCheckpointTimeout();
        // Ensure meet the strictest requirements
        semantic = FlinkKafkaProducer.Semantic.EXACTLY_ONCE;
    }

    public KafkaProducerParamVerifier(Properties kafkaProps,
                                      CheckpointConfig checkpointConfig,
                                      FlinkKafkaProducer.Semantic semantic) {
        this(kafkaProps, checkpointConfig);
        this.semantic = semantic;
    }

    // This condition ensure flink stay running when kafka reach critical state (healthy state but high pressure)
    // So we set all flink job must be meet with this condition.
    public boolean hardVerify() {
        checkArgument(cpInterval >= cpTimeout, "cpTimeout cannot be larger than cpInterval!");

        // It must be satisfied, which defined by Kafka officially.
        checkArgument( deliveryTimeout >= requestTimeout + linger, "Kafka deliveryTimeout must >= (requestTimeout + linger)! ");
        // Ensure once retry success.
        checkArgument( cpTimeout >= maxBlock + deliveryTimeout, "One retry must satisfy cpTimeout >= maxBlock + deliveryTimeout! ");

        // Transaction.timeout.ms is just work for EXACTLY_ONCE
        if (semantic == FlinkKafkaProducer.Semantic.EXACTLY_ONCE) {
            // Ensure transaction not killed by checkpoint timeout.
            checkArgument( transactionTimeout <= cpTimeout, "Flink cpTimeout must >= Kafka transactionTimeout! ");
            // Ensure transaction not killed by deliveryTimeout.
            checkArgument( transactionTimeout > deliveryTimeout - linger, "Kafka transactionTimeout must >= (deliveryTimeout - linger)! ");
            checkArgument( cpTimeout >= maxBlock + linger + transactionTimeout,
                    "One retry must satisfy cpTimeout >= maxBlock + linger + transactionTimeout! ");
        }
        return true;
    }

    // This condition ensure flink stay running when kafka unreachable and achieve max retry times. It could be set in
    // some critical flink job like fee related.
    public boolean softVerify() {
        hardVerify();
        // Transaction.timeout.ms is work for EXACTLY_ONCE
        if (semantic == FlinkKafkaProducer.Semantic.EXACTLY_ONCE) {
            // This condition should apply EXACTLY_ONCE && retryCallback. We just could apply one of them.
            checkArgument( transactionTimeout > actualCallbackRetryTimes * (maxBlock + deliveryTimeout),
                    "actualCallbackRetryTimes retry must satisfy transactionTimeout > actualCallbackRetryTimes * (maxBlock + deliveryTimeout)! ");

        }
        checkArgument(  cpTimeout >= actualCallbackRetryTimes * (maxBlock + deliveryTimeout),
                "actualCallbackRetryTimes retry must satisfy cpTimeout >= actualCallbackRetryTimes * (maxBlock + deliveryTimeout)! ");
        return true;
    }
}
