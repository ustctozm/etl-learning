package net.qihoo.ads.flink.kafka;

import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.util.Preconditions;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class BalancedFlinkKafkaPartitioner<T> extends FlinkKafkaPartitioner<T> {

    private static final long serialVersionUID = 2361934354380286638L;
    private int parallelInstanceId;
    private int parallelInstancesCount;

    @Override
    public void open(int parallelInstanceId, int parallelInstancesCount) {
        Preconditions.checkArgument(
                parallelInstanceId >= 0, "Id of this subtask cannot be negative.");
        Preconditions.checkArgument(
                parallelInstancesCount > 0, "Number of subtasks must be larger than 0.");
        this.parallelInstanceId = parallelInstanceId;
        this.parallelInstancesCount = parallelInstancesCount;
    }
    @Override
    public int partition(T record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
        Preconditions.checkArgument(
                partitions.length > 0, "Partitions of this topic must be larger than 0.");
        int factor = (int) Math.ceil((double) partitions.length / parallelInstancesCount);
        Random random = ThreadLocalRandom.current();
        int randomValue = random.nextInt(factor);
        return partitions[(parallelInstanceId + randomValue * parallelInstancesCount) % partitions.length];
    }
}
