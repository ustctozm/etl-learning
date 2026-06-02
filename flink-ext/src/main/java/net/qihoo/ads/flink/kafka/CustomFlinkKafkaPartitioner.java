package net.qihoo.ads.flink.kafka;

import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 *  If some kafka partition unreachable, We could set unhealthy partition number into List<Integer>, Then
 *  use constructor method of CustomFlinkKafkaPartitioner and this List<Integer> to generate custom Kafka partitioner.
 *  Ensure Flink job keeping continuous running state.
 *
 * <p>Example:
 *
 * <pre>
 *   public static FlinkKafkaProducer<byte[]> createCustomKafkaProducer(Properties kafkaProps,
 *           String outputTopic,
 *           FlinkKafkaProducer.Semantic semantic) {
 *
 *     FlinkKafkaPartitioner<byte[]> flinkKafkaPartitioner = new CustomFlinkKafkaPartitioner<>(new ArrayList<Integer>() {{
 *       add(0);add(1);add(2);}});
 *
 *     return new FlinkKafkaProducer<>(
 *                 outputTopic,
 *                 (SerializationSchema<byte[]>) (element) -> element,
 *                 kafkaProps,
 *                 flinkKafkaPartitioner,
 *                 semantic,
 *                 DEFAULT_KAFKA_PRODUCERS_POOL_SIZE);
 *
 *     }
 * </pre>
 *   Usage: kafkaStream.addSink(createCustomKafkaProducer(props, topic,  semantic));
 */
public class CustomFlinkKafkaPartitioner<T> extends FlinkKafkaPartitioner<T> {

    private static final long serialVersionUID = -8019266918926904620L;
    private final List<Integer> blockedPartitions;
    private int parallelInstanceId;

    public CustomFlinkKafkaPartitioner(List<Integer> blockedPartitions) {
        this.blockedPartitions = blockedPartitions;
    }

    @Override
    public void open(int parallelInstanceId, int parallelInstancesCount) {
        Preconditions.checkArgument(
                parallelInstanceId >= 0, "Id of this subtask cannot be negative.");
        Preconditions.checkArgument(
                parallelInstancesCount > 0, "Number of subtasks must be larger than 0.");
        this.parallelInstanceId = parallelInstanceId;
    }

    @Override
    public int partition(T record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
        // Copying from FlinkFixedPartitioner
        int targetPartition = partitions[parallelInstanceId % partitions.length];

        // Custom code, remove bad kafka partition from blockedPartitions
        if (blockedPartitions!=null && blockedPartitions.contains(targetPartition)) {
            List<Integer> validPartitions = Arrays.stream(partitions)
                    .boxed()
                    .filter(i -> !blockedPartitions.contains(i))
                    .collect(Collectors.toList());
            // this will throw IllegalArgumentException in generate ProducerRecord
            if (validPartitions.size() == 0) {
                return -1;
            }
            // Ensure a certain taskSlot using minimize partitions of the topic.
            targetPartition = validPartitions.get(parallelInstanceId % validPartitions.size());
        }
        return targetPartition;
    }

}
