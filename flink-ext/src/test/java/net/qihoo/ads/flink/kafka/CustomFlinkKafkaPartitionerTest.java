package net.qihoo.ads.flink.kafka;

import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class CustomFlinkKafkaPartitionerTest {

    @Test
    public  void partitionerTest() {

        int[] topicPartitions ={0, 1, 2};

        List<Integer> badPartitions = new ArrayList<>();
        badPartitions.add(2);

        // Flink default method to assign partitions
        FlinkKafkaPartitioner<String> flinkKafkaPartitioner = new FlinkFixedPartitioner<>();
        flinkKafkaPartitioner.open(2, 3);
        int fixedPartition = flinkKafkaPartitioner.partition(null, null, null, "", topicPartitions);
        assertEquals(fixedPartition, 2);

        CustomFlinkKafkaPartitioner<String> customFlinkKafkaPartitioner = new CustomFlinkKafkaPartitioner<String>(badPartitions);
        customFlinkKafkaPartitioner.open(2, 3);
        int customPartition = customFlinkKafkaPartitioner.partition(null, null, null, "", topicPartitions);
        // change to healthy partition
        assertEquals(customPartition, 0);

        CustomFlinkKafkaPartitioner<String> customFlinkKafkaPartitioner1 = new CustomFlinkKafkaPartitioner<String>(badPartitions);
        customFlinkKafkaPartitioner1.open(2, 3);
        int customPartition1 = customFlinkKafkaPartitioner1.partition(null, null, null, "", topicPartitions);
        // all kafka record's partitions for slot 2 are fixed, When getting bad partition.
        assertEquals(customPartition1, 0);

        badPartitions.add(0);
        badPartitions.add(1);
        CustomFlinkKafkaPartitioner<String> customFlinkKafkaPartitioner2 = new CustomFlinkKafkaPartitioner<String>(badPartitions);
        int customPartition2 = customFlinkKafkaPartitioner2.partition(null, null, null, "", topicPartitions);
        assertEquals(customPartition2, -1);

    }

    @Test
    public void partitionerSlotWithNoBadPartition() {

        int[] topicPartitions ={0, 1, 2};

        List<Integer> badPartitions = new ArrayList<>();
        badPartitions.add(2);

        // Flink default method to assign partitions
        FlinkKafkaPartitioner<String> flinkKafkaPartitioner = new FlinkFixedPartitioner<>();
        // the partition of this slot 1 is healthy
        flinkKafkaPartitioner.open(1, 3);
        int fixedPartition = flinkKafkaPartitioner.partition(null, null, null, "", topicPartitions);
        assertEquals(fixedPartition, 1);

        CustomFlinkKafkaPartitioner<String> customFlinkKafkaPartitioner = new CustomFlinkKafkaPartitioner<String>(badPartitions);
        customFlinkKafkaPartitioner.open(1, 3);
        int customPartition = customFlinkKafkaPartitioner.partition(null, null, null, "", topicPartitions);
        // bad partition has no effect fot slot 1
        assertEquals(customPartition, 1);
    }

}
