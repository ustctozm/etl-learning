package net.qihoo.ads.flink.kafka;

import org.apache.flink.shaded.guava31.com.google.common.collect.Sets;
import org.apache.flink.util.Preconditions;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.util.Preconditions.checkArgument;

public class TopicPartitions {

    /** All partitions of each topic. */
    public final HashMap<String, int[]> topicPartitionsMap = new HashMap<>();
    /** All healthy partitions of each topic. */
    public final HashMap<String, int[]> healthyTopicPartitionsMap = new HashMap<>();
    /** Bad partitions of each topic. [Topic, [checkPointId, partitions]]  */
    public final ConcurrentHashMap<String, Map<Long, Set<Integer>>> topicBadPartitionsMap = new ConcurrentHashMap<>();

    /** When badPartitions exist actualCleanBadPartitionsCpTimes cp interval, attempt to recover these partitions. */
    public final int actualCleanBadPartitionsCpTimes;
    private long checkPointId = 0L;
    public TopicPartitions(int actualCleanBadPartitionsCpTimes) {
        checkArgument(actualCleanBadPartitionsCpTimes > 0, "Kafka retry parameter cannot be negative!");
        this.actualCleanBadPartitionsCpTimes = actualCleanBadPartitionsCpTimes;
    }

    /**
     * This method is called synchronously in FlinkKafkaProducer.invoke() When first kafka produce record comes.
     * So topicPartitionsMap && healthyTopicPartitionsMap must have been set before send thread enter into callback.
     */
    public void addPartitionsForTopic(String topic, int[] partitions) {
        Preconditions.checkArgument(
                topic != null, "Topic cannot be null.");
        Preconditions.checkArgument(
                partitions.length > 0, "partitions length must be > 0.");
        topicPartitionsMap.put(topic, partitions);
        healthyTopicPartitionsMap.put(topic, partitions);
    }

    public void addBadPartition(String topic, int badPartition) {
        Preconditions.checkArgument(
                topic != null, "Topic cannot be null.");
        Preconditions.checkArgument(
                badPartition >= 0, "partition must be >= 0.");
        if (!topicBadPartitionsMap.containsKey(topic)
                || !topicBadPartitionsMap.get(topic).containsKey(checkPointId)
                || !topicBadPartitionsMap.get(topic).get(checkPointId).contains(badPartition)) {

            topicBadPartitionsMap.compute(topic, (k, cpMap) -> {

                if (!healthyTopicPartitionsMap.containsKey(topic)) {
                    throw new RuntimeException("Send record without executing addPartitionsForTopic!");
                } else if (healthyTopicPartitionsMap.get(topic) == null){
                    throw new RuntimeException("FlinkKafkaProducer.getPartitionsByTopic result is null!");
                } else {
                    int[] newHealthyPartitions = Arrays.stream(healthyTopicPartitionsMap.get(topic)).filter(x -> x != badPartition).sorted().toArray();
                    healthyTopicPartitionsMap.put(topic, newHealthyPartitions);
                }

                if (cpMap != null) {
                    cpMap.compute(checkPointId, (cpId, value) -> {
                        if (value != null) {
                            value.add(badPartition);
                            return value;
                        } else {
                            return Sets.newHashSet(badPartition);
                        }
                    });
                    return cpMap;
                } else {
                    HashMap<Long, Set<Integer>> hashMap = new HashMap<>();
                    hashMap.put(checkPointId, Sets.newHashSet(badPartition));
                    return hashMap;
                }
            });
        }
    }

    public boolean isHealthyForTopic(String topic) {
        Preconditions.checkArgument(
                topic != null, "Topic cannot be null.");
        return !topicBadPartitionsMap.containsKey(topic)
                || topicBadPartitionsMap.get(topic) == null
                || topicBadPartitionsMap.get(topic).isEmpty();
    }

    public String getBadPartitionInfo() {
        StringBuilder stringBuilder = new StringBuilder();
        topicBadPartitionsMap.forEach((key, value) -> {
            stringBuilder.append("Topic=").append(key);
            if (!value.isEmpty()) {
                Set<Integer> allBadPartitions = new HashSet<>();
                for (Set<Integer> set : value.values()) {
                    allBadPartitions.addAll(set);
                }
                stringBuilder.append(", badPartitions=").append(allBadPartitions).append("; ");
            }
        });
        return stringBuilder.length() > 0 ? stringBuilder.toString() : null;
    }

    private void flushHealthyTopicPartitionsMap(String topic) {
        Preconditions.checkArgument(
                topic != null, "Topic cannot be null.");
        if (topicPartitionsMap.isEmpty() || !topicPartitionsMap.containsKey(topic)) {
            throw new RuntimeException("getPartitionsByTopic's partitions for topic=" + topic + " is empty!");
        }
        int[] partitions = topicPartitionsMap.get(topic);
        Set<Integer> allBadPartitions = new HashSet<>();
        topicBadPartitionsMap.computeIfPresent(topic, (s, longSetMap) -> {
            longSetMap.values().forEach(allBadPartitions::addAll);
            return longSetMap;
        });

        if (allBadPartitions.isEmpty()) {
            healthyTopicPartitionsMap.put(topic, Arrays.stream(partitions).sorted().toArray());
        } else {
            healthyTopicPartitionsMap.put(topic, Arrays.stream(partitions).filter(i -> !allBadPartitions.contains(i)).sorted().toArray());
        }
    }

    /**
     * 'return null' enable that {@link #addPartitionsForTopic(String, int[] partitions)} method just executed only once.
     *  When first kafka produce record enter to FlinkKafkaProducer.invoke()
     */
    public int[] getAvailablePartitions(String topic) {
        Preconditions.checkArgument(
                topic != null, "Topic cannot be null.");
       if (healthyTopicPartitionsMap.isEmpty() || !healthyTopicPartitionsMap.containsKey(topic)) {
           return null;
       } else {
           return healthyTopicPartitionsMap.get(topic);
       }
    }

    /**
     * This method should be called in FlinkKafkaProducer.snapshotState() which super TwoPhaseCommitSinkFunction..snapshotState()
     *  And all callbacks must have been executed in TwoPhaseCommitSinkFunction.snapshotState(). Otherwise it will throw exception.
     *  So it must be thread safe compare with {@link #addBadPartition(String, int)} method.
     */
    public void cleanBadPartition() {
        if (actualCleanBadPartitionsCpTimes <= 1) {
            topicBadPartitionsMap.clear();
        } else {
            // clean bad partitions for old checkPointId
            for (Map.Entry<String, Map<Long, Set<Integer>>> entry : topicBadPartitionsMap.entrySet()) {
                Map<Long, Set<Integer>> value = entry.getValue();
                if (value != null && !value.isEmpty()) {
                    value.keySet().removeIf(cpId -> checkPointId - cpId >= actualCleanBadPartitionsCpTimes);
                }
                flushHealthyTopicPartitionsMap(entry.getKey());
            }
            topicBadPartitionsMap.entrySet().removeIf(entry -> entry.getValue() == null || entry.getValue().isEmpty());
        }
    }

    public boolean isBadPartition(String topic, Integer partition) {
        Preconditions.checkArgument(
                topic != null, "Topic cannot be null.");
        Preconditions.checkArgument(
                partition != null && partition >= 0, "partition must be not null and a positive number!");
        if (healthyTopicPartitionsMap.isEmpty()
                || !healthyTopicPartitionsMap.containsKey(topic)
                || healthyTopicPartitionsMap.get(topic).length == 0) {
            throw new RuntimeException("We have no healthy partitions for topic=" + topic + "!");
        }
        return Arrays.stream(healthyTopicPartitionsMap.get(topic)).noneMatch(x -> x==partition);
    }

    /**
     * When snapshotState(context) executed, We set checkPointId = context.getCheckpointId().
     * It is not accurately, TopicPartitions may exist cp1, cp1-1 these two checkPointId in one snapshotState cycle.
     * Because FlinkKafkaProducer is not extended AbstractUdfStreamOperator method, OtherWise we could get it from
     * prepareSnapshotPreBarrier(long checkpointId) method.
     * So If actualCleanBadPartitionsCpTimes set to 2, When first cp comes, badPartition may be cleaned.
     */
    public void setCheckPointId(long checkPointId) {
        this.checkPointId = checkPointId;
    }

    public int[] getTopicPartitions(String topic) {
        Preconditions.checkArgument(
                topic != null, "Topic cannot be null.");
        return topicPartitionsMap.getOrDefault(topic, null);
    }

    // Thread unsafe!, Just for ut now
    public void cleanALllBadPartition(String topic) {
        Preconditions.checkArgument(
                topic != null, "Topic cannot be null.");
        topicBadPartitionsMap.remove(topic);
        flushHealthyTopicPartitionsMap(topic);
    }

}
