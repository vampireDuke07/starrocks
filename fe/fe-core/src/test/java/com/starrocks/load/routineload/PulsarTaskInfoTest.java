package com.starrocks.load.routineload;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.Pair;
import com.starrocks.common.UserException;
import com.starrocks.common.util.KafkaUtil;
import com.starrocks.common.util.PulsarUtil;
import mockit.Injectable;
import mockit.Mock;
import mockit.MockUp;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class PulsarTaskInfoTest {

    @Test
    public void testReadyToExecute(@Injectable PulsarRoutineLoadJob pulsarRoutineLoadJob) throws Exception {
        new MockUp<RoutineLoadManager>() {
            @Mock
            public RoutineLoadJob getJob(long jobId) {
                return pulsarRoutineLoadJob;
            }
        };

        new MockUp<PulsarUtil>() {
            @Mock
            public Map<Integer, Long> getLatestOffsets(String serverUrl, String topic,
                                                       ImmutableMap<String, String> properties,
                                                       List<Integer> partitions) throws UserException {
                Map<Integer, Long> offsets = Maps.newHashMap();
                offsets.put(0, 100L);
                offsets.put(1, 100L);
                return offsets;
            }
        };

        Map<Integer, Long> offset1 = Maps.newHashMap();
        offset1.put(0, 99L);
        PulsarTaskInfo pulsarTaskInfo1 = new PulsarTaskInfo(UUID.randomUUID(),
                1L,
                "cluster",
                System.currentTimeMillis(),
                System.currentTimeMillis(),
                offset1);
        Assert.assertTrue(pulsarTaskInfo1.readyToExecute());

        Map<Integer, Long> offset2 = Maps.newHashMap();
        offset1.put(0, 100L);
        PulsarTaskInfo pulsarTaskInfo2 = new PulsarTaskInfo(UUID.randomUUID(),
                1L,
                "cluster",
                System.currentTimeMillis(),
                System.currentTimeMillis(),
                offset2);
        Assert.assertFalse(pulsarTaskInfo2.readyToExecute());
    }

    @Test
    public void testProgressKeepUp(@Injectable PulsarRoutineLoadJob pulsarRoutineLoadJob) throws Exception {
        new MockUp<RoutineLoadManager>() {
            @Mock
            public RoutineLoadJob getJob(long jobId) {
                return pulsarRoutineLoadJob;
            }
        };

        new MockUp<PulsarUtil>() {
            @Mock
            public Map<Integer, Long> getLatestOffsets(String serverUrl, String topic,
                                                       ImmutableMap<String, String> properties,
                                                       List<Integer> partitions) throws UserException {
                Map<Integer, Long> offsets = Maps.newHashMap();
                offsets.put(0, 100L);
                offsets.put(1, 100L);
                return offsets;
            }
        };

        Map<Integer, Long> offset = Maps.newHashMap();
        offset.put(0, 99L);
        PulsarTaskInfo pulsarTaskInfo = new PulsarTaskInfo(UUID.randomUUID(),
                1L,
                "cluster",
                System.currentTimeMillis(),
                System.currentTimeMillis(),
                offset);
        // call readyExecute to cache latestPartOffset
        pulsarTaskInfo.readyToExecute();

        PulsarProgress pulsarProgress = new PulsarProgress();
        pulsarProgress.addPartitionOffset(new Pair<>(0, 98L));
        pulsarProgress.addPartitionOffset(new Pair<>(1, 98L));
        Assert.assertFalse(pulsarTaskInfo.isProgressKeepUp(pulsarProgress));

        pulsarProgress.modifyOffset(Lists.newArrayList(new Pair<>(0, 99L)));
        Assert.assertFalse(pulsarTaskInfo.isProgressKeepUp(pulsarProgress));

        pulsarProgress.modifyOffset(Lists.newArrayList(new Pair<>(1, 99L)));
        Assert.assertTrue(pulsarTaskInfo.isProgressKeepUp(pulsarProgress));
    }
}
