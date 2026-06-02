package net.qihoo.ads.flink.kafka;

import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

public class KafkaProducerParamVerifierTest {

    @Test
    public void hardVerifyTest() {
        Properties kafkaProp = new Properties();
        // necessary params
        kafkaProp.setProperty("bootstrap.servers", "test-for-ut.qihoo.net:9092");
        kafkaProp.setProperty("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        kafkaProp.setProperty("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

        CheckpointConfig checkpointConfig = new CheckpointConfig();
        checkpointConfig.setCheckpointInterval(300000);

        KafkaProducerParamVerifier verifier = new KafkaProducerParamVerifier(kafkaProp, checkpointConfig);
        // KafkaConfig default params
        Assert.assertEquals(verifier.transactionTimeout, 60000);
        Assert.assertEquals(verifier.requestTimeout, 30000);
        Assert.assertEquals(verifier.deliveryTimeout, 120000);
        Assert.assertEquals(verifier.linger, 0);
        Assert.assertEquals(verifier.maxBlock, 60000);
        Assert.assertEquals(verifier.cpTimeout, 600000);

        // default cp timeout
        try {
            Assert.assertTrue(verifier.hardVerify());
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "cpTimeout cannot be larger than cpInterval!");
        }

        // Correct checkpoint timeout
        checkpointConfig.setCheckpointTimeout(300000);
        KafkaProducerParamVerifier verifier1 = new KafkaProducerParamVerifier(kafkaProp, checkpointConfig);
        try {
            Assert.assertTrue(verifier1.hardVerify());
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Kafka transactionTimeout must >= (deliveryTimeout - linger)! ");
        }

        // Add transaction timeout
        kafkaProp.setProperty("transaction.timeout.ms", "300000");
        KafkaProducerParamVerifier verifier2 = new KafkaProducerParamVerifier(kafkaProp, checkpointConfig);
        try {
            Assert.assertTrue(verifier2.hardVerify());
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "One retry must satisfy cpTimeout >= maxBlock + linger + transactionTimeout! ");
        }

        // Justify transaction timeout
        kafkaProp.setProperty("transaction.timeout.ms", "240000");
        KafkaProducerParamVerifier verifier3 = new KafkaProducerParamVerifier(kafkaProp, checkpointConfig);
        Assert.assertTrue(verifier3.hardVerify());

        //--------------------------------------soft verify---------------------------------
        try {
            Assert.assertTrue(verifier3.softVerify());
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "actualCallbackRetryTimes retry must satisfy transactionTimeout > actualCallbackRetryTimes * (maxBlock + deliveryTimeout)! ");
        }
        // Add transaction timeout
        kafkaProp.setProperty("transaction.timeout.ms", "900000");
        KafkaProducerParamVerifier verifier4 = new KafkaProducerParamVerifier(kafkaProp, checkpointConfig);
        try {
            Assert.assertTrue(verifier4.softVerify());
        } catch (Exception e) {
            Assert.assertEquals(e.getMessage(), "Flink cpTimeout must >= Kafka transactionTimeout! ");
        }

        // Add cpTimeout timeout
        checkpointConfig.setCheckpointTimeout(960000);
        checkpointConfig.setCheckpointInterval(960000);
        KafkaProducerParamVerifier verifier5 = new KafkaProducerParamVerifier(kafkaProp, checkpointConfig);
        Assert.assertTrue(verifier5.softVerify());

    }
}
