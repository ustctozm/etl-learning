package net.qihoo.ads.flink.kafka;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class TopicPartitionsTest {

    @Test
    public void topicPartitionsActionTest() {
        int actualCleanBadPartitionsCpTimes = 6;
        TopicPartitions topicPartitions = new TopicPartitions(actualCleanBadPartitionsCpTimes);
        topicPartitions.addPartitionsForTopic("click", new int[]{0, 1, 2, 3, 4, 5});
        topicPartitions.addPartitionsForTopic("show", new int[]{0, 1, 2, 3, 4, 5});

        // mock partition unavailable
        long checkPointId = 1L;
        topicPartitions.setCheckPointId(checkPointId);
        topicPartitions.addBadPartition("click", 2);
        Assert.assertEquals(Arrays.toString(topicPartitions.getAvailablePartitions("click")), "[0, 1, 3, 4, 5]");

        // mock all partitions unavailable
        topicPartitions.addBadPartition("show", 0);
        topicPartitions.addBadPartition("show", 1);
        topicPartitions.addBadPartition("show", 2);
        topicPartitions.addBadPartition("show", 3);
        topicPartitions.addBadPartition("show", 4);
        Assert.assertTrue(topicPartitions.isBadPartition("show", 0));
        Assert.assertEquals(Arrays.toString(topicPartitions.getAvailablePartitions("show")), "[5]");
        String result = topicPartitions.getBadPartitionInfo();
        Assert.assertEquals(result, "Topic=show, badPartitions=[0, 1, 2, 3, 4]; Topic=click, badPartitions=[2]; ");
        // when achieve actualCleanBadPartitionsCpTimes, clean all bad partitions
        checkPointId = 6;
        topicPartitions.setCheckPointId(checkPointId);
        topicPartitions.cleanBadPartition();
        Assert.assertEquals(Arrays.toString(topicPartitions.getAvailablePartitions("click")), "[0, 1, 3, 4, 5]");
        Assert.assertTrue(topicPartitions.isBadPartition("click", 2));
        checkPointId = 7;
        topicPartitions.setCheckPointId(checkPointId);
        topicPartitions.cleanBadPartition();
        Assert.assertFalse(topicPartitions.isBadPartition("click", 2));
        Assert.assertEquals(Arrays.toString(topicPartitions.getAvailablePartitions("click")), "[0, 1, 2, 3, 4, 5]");

        Assert.assertFalse(topicPartitions.isBadPartition("show", 0));
        // all badPartitions has been cleaned
        result = topicPartitions.getBadPartitionInfo();
        Assert.assertNull(result);

        // test cleanALllBadPartition
        topicPartitions.addBadPartition("click", 3);
        topicPartitions.addBadPartition("click", 4);
        Assert.assertEquals(Arrays.toString(topicPartitions.getAvailablePartitions("click")), "[0, 1, 2, 5]");
        topicPartitions.cleanALllBadPartition("click");
        Assert.assertEquals(Arrays.toString(topicPartitions.getAvailablePartitions("click")), "[0, 1, 2, 3, 4, 5]");

        topicPartitions.addPartitionsForTopic("cpmshow", new int[] {4, 5, 1, 0, 3, 2});
        topicPartitions.addBadPartition("cpmshow", 4);
        Assert.assertEquals(Arrays.toString(topicPartitions.getAvailablePartitions("cpmshow")), "[0, 1, 2, 3, 5]");
        topicPartitions.cleanALllBadPartition("cpmshow");
        Assert.assertEquals(Arrays.toString(topicPartitions.getAvailablePartitions("cpmshow")), "[0, 1, 2, 3, 4, 5]");
    }
}
