package net.qihoo.ads.flink.concurrent;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.accumulators.*;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.externalresource.ExternalResourceInfo;
import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.*;
import org.apache.flink.metrics.groups.OperatorMetricGroup;
import org.apache.flink.shaded.guava31.com.google.common.hash.Hashing;
import org.apache.flink.util.ChildFirstClassLoader;
import org.junit.Test;

import java.io.Serializable;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;

import static org.junit.Assert.*;

public class JobSingletonFactoryTest {
  private static String lastReleaseHookName = null;
  private static Runnable lastReleaseHook = null;
  private static final JobID jid = JobID.fromHexString("fd72014d4c864993a2e5a9287b4a9c5d");
  static RuntimeContext getRuntimeContext(final String taskName, final ClassLoader cl) {
    return new RuntimeContext() {
           @Override public JobID getJobId() { return jid; }
           @Override public String getTaskName() { return taskName; }
           @Override public int getNumberOfParallelSubtasks() { return 123; }
           @Override public ClassLoader getUserCodeClassLoader() { return cl; }
           @Override
           public void registerUserCodeClassLoaderReleaseHookIfAbsent(String releaseHookName, Runnable releaseHook) {
             lastReleaseHookName = releaseHookName;
             lastReleaseHook = releaseHook;
           }

           @Override public OperatorMetricGroup getMetricGroup() { throw new UnsupportedOperationException(); }
           @Override public int getMaxNumberOfParallelSubtasks() { throw new UnsupportedOperationException(); }
           @Override public int getIndexOfThisSubtask() { throw new UnsupportedOperationException(); }
           @Override public int getAttemptNumber() { throw new UnsupportedOperationException(); }
           @Override public String getTaskNameWithSubtasks() { throw new UnsupportedOperationException(); }
           @Override public ExecutionConfig getExecutionConfig() { throw new UnsupportedOperationException(); }
           @Override public <V, A extends Serializable> void addAccumulator(String name, Accumulator<V, A> accumulator) { throw new UnsupportedOperationException(); }
           @Override public <V, A extends Serializable> Accumulator<V, A> getAccumulator(String name) { throw new UnsupportedOperationException(); }
           @Override public IntCounter getIntCounter(String name) { throw new UnsupportedOperationException(); }
           @Override public LongCounter getLongCounter(String name) { throw new UnsupportedOperationException(); }
           @Override public DoubleCounter getDoubleCounter(String name) { throw new UnsupportedOperationException(); }
           @Override public Histogram getHistogram(String name) { throw new UnsupportedOperationException(); }
           @Override public boolean hasBroadcastVariable(String name) { throw new UnsupportedOperationException(); }
           @Override public <RT> List<RT> getBroadcastVariable(String name) { throw new UnsupportedOperationException(); }
           @Override public <T, C> C getBroadcastVariableWithInitializer(String name, BroadcastVariableInitializer<T, C> initializer) { throw new UnsupportedOperationException(); }
           @Override public DistributedCache getDistributedCache() { throw new UnsupportedOperationException(); }
           @Override public <T> ValueState<T> getState(ValueStateDescriptor<T> stateProperties)  { throw new UnsupportedOperationException(); }
           @Override public <T> ListState<T> getListState(ListStateDescriptor<T> stateProperties) { throw new UnsupportedOperationException(); }
           @Override public <T> ReducingState<T> getReducingState(ReducingStateDescriptor<T> stateProperties) { throw new UnsupportedOperationException(); }
           @Override public <IN, ACC, OUT> AggregatingState<IN, OUT> getAggregatingState(AggregatingStateDescriptor<IN, ACC, OUT> stateProperties) { throw new UnsupportedOperationException(); }
           @Override public Set<ExternalResourceInfo> getExternalResourceInfos(String resourceName) { throw new UnsupportedOperationException(); }
           @Override public <UK, UV> MapState<UK, UV> getMapState(MapStateDescriptor<UK, UV> stateProperties) { throw new UnsupportedOperationException(); }
    };
  }

  private static class AutoCloseableWith implements AutoCloseable {
    final Consumer<AutoCloseable> closer;
    AutoCloseableWith(final Consumer<AutoCloseable> closer) {
      this.closer = closer;
    }
    @Override
    public void close() throws Exception {
      if (closer != null) {
        closer.accept(this);
      }
    }
  }

  @Test
  public void testAutoCloseable() throws Exception {
    final ClassLoader userCL = new ChildFirstClassLoader(new URL[0], JobSingletonInstances.class.getClassLoader(), new String[0], t -> {});
    final RuntimeContext ctx = getRuntimeContext("task", userCL);
    final RuntimeContext ctxLongTask = getRuntimeContext("FEDCBA9876543210.task", userCL);

    lastReleaseHookName = null;
    lastReleaseHook = null;
    final AutoCloseable w0 = JobSingletonFactory.withSupplier(() -> new AutoCloseableWith(null), ctx, "w0", false).instance();
    assertEquals("Release singletons created by JobSingletonFactory for job " + jid, lastReleaseHookName);
    assertNotNull(lastReleaseHook);

    final Runnable firstHook = lastReleaseHook;

    lastReleaseHookName = null;
    lastReleaseHook = null;
    final AutoCloseable w0_2 = JobSingletonFactory.withSupplier(() -> new AutoCloseableWith(null), ctx, "w0", false).instance();
    assertNull(lastReleaseHookName);
    assertNull(lastReleaseHook);
    assertSame(w0, w0_2);

    assertEquals(1, JobSingletonInstances.getInstance(userCL).resourceStateMap.size());
    assertEquals("job$w0", JobSingletonInstances.getInstance(userCL).orders.getLast());

    lastReleaseHookName = null;
    lastReleaseHook = null;
    final AutoCloseable w1 = JobSingletonFactory.withSupplier(() -> new AutoCloseableWith(null), ctx, "w1", false).instance();
    assertEquals("Release singletons created by JobSingletonFactory for job " + jid, lastReleaseHookName);
    assertNotNull(lastReleaseHook);
    assertNotSame(w0, w1);
    assertEquals(2, JobSingletonInstances.getInstance(userCL).resourceStateMap.size());
    assertEquals("job$w1", JobSingletonInstances.getInstance(userCL).orders.getLast());

    final AutoCloseable w0_operator =
          JobSingletonFactory.withSupplier(() -> new AutoCloseableWith(null), ctx, "w0", true).instance();
    assertNotSame(w0, w0_operator);
    assertEquals(3, JobSingletonInstances.getInstance(userCL).resourceStateMap.size());
    assertEquals("operator$123$4$task$w0", JobSingletonInstances.getInstance(userCL).orders.getLast());

    final AutoCloseable o2 =
          JobSingletonFactory.withSupplier(() -> new AutoCloseableWith(null), ctxLongTask, "o2", true).instance();
    assertEquals(4, JobSingletonInstances.getInstance(userCL).resourceStateMap.size());
    final String o2_key = "operator$123$21$"
                        + Hashing.murmur3_128().hashString("FEDCBA9876543210.task", StandardCharsets.UTF_8)
                        + "$FEDCBA9876543210$o2";
    assertEquals(o2_key, JobSingletonInstances.getInstance(userCL).orders.getLast());

    final AutoCloseable o2_2 =
          JobSingletonFactory.withSupplier(() -> new AutoCloseableWith(null), ctxLongTask, "o2", true).instance();
    assertSame(o2, o2_2);

    // call release hook
    firstHook.run();
    assertEquals(0, JobSingletonInstances.getInstance(userCL).resourceStateMap.size());
    assertEquals(0, JobSingletonInstances.getInstance(userCL).orders.size());

    lastReleaseHookName = null;
    lastReleaseHook = null;
    final AutoCloseable w0_3 = JobSingletonFactory.withSupplier(() -> new AutoCloseableWith(null), ctx, "w0", false).instance();
    assertNull(w0_3);
    assertNull(lastReleaseHookName);
    final AutoCloseable o2_3 =
          JobSingletonFactory.withSupplier(() -> new AutoCloseableWith(null), ctxLongTask, "o2", false).instance();
    assertNull(o2_3);
    assertNull(lastReleaseHookName);

    final AutoCloseable w3 = JobSingletonFactory.withSupplier(() -> new AutoCloseableWith(null), ctx, "w3", false).instance();
    assertNull(w3);
    assertNull(lastReleaseHookName);

    final AutoCloseable o4 =
          JobSingletonFactory.withSupplier(() -> new AutoCloseableWith(null), ctxLongTask, "o4", true).instance();
    assertNull(o4);
    assertNull(lastReleaseHookName);
  }

  boolean closerRun1 = false;
  boolean closerRun2 = false;
  boolean closerRun3 = false;

  @Test
  public void testCloser() throws Exception {
    final ClassLoader userCL1 = new ChildFirstClassLoader(new URL[0], JobSingletonInstances.class.getClassLoader(), new String[0], t -> {});
    final RuntimeContext ctx1 = getRuntimeContext("task", userCL1);

    lastReleaseHookName = null;
    lastReleaseHook = null;
    closerRun1 = false;
    closerRun2 = false;
    closerRun3 = false;
    final AutoCloseable w0 = JobSingletonFactory
                             .withSupplierAndCloser(() -> new AutoCloseableWith(c -> closerRun1 = true), autoCloseableWith -> closerRun2 = true, ctx1, "w0", false)
                             .instance();
    final AutoCloseable w0_2 = JobSingletonFactory
                             .withSupplierAndCloser(() -> new AutoCloseableWith(c -> closerRun3 = true), null, ctx1, "w0", false)
                             .instance();
    lastReleaseHook.run();
    // AutoCloseable type will ignore `withCloser()`
    assertSame(w0, w0_2);
    assertTrue(closerRun1);
    assertFalse(closerRun2);
    assertFalse(closerRun3);

    final ClassLoader userCL2 = new ChildFirstClassLoader(new URL[0], JobSingletonInstances.class.getClassLoader(), new String[0], t -> {});
    final RuntimeContext ctx2 = getRuntimeContext("task", userCL2);
    lastReleaseHookName = null;
    lastReleaseHook = null;
    closerRun1 = false;
    closerRun2 = false;
    closerRun3 = false;
    final AutoCloseable w1 = JobSingletonFactory
                       .withSupplierAndCloser(() -> new AutoCloseableWith(c -> closerRun1=true), autoCloseableWith -> closerRun1 = true, ctx2, "w1", false)
                       .instance();
    lastReleaseHook.run();
    assertTrue("Normal type closer works", closerRun1);

    closerRun1 = false;
    final ClassLoader userCL3 = new ChildFirstClassLoader(new URL[0], JobSingletonInstances.class.getClassLoader(), new String[0], t -> {});
    final RuntimeContext ctx3 = getRuntimeContext("task", userCL3);

    JobSingletonFactory<AutoCloseable> integerJobSingletonFactory = JobSingletonFactory
            .withSupplierAndCloser(() -> new AutoCloseableWith(c -> closerRun1 = true), null, ctx3, "w1", false);
    final AutoCloseable w2_0 = integerJobSingletonFactory.instance();
    final AutoCloseable w2_1 = integerJobSingletonFactory.instance();
    assertEquals(w2_0, w2_1);
    integerJobSingletonFactory.close();
    assertFalse("Normal type closer works", closerRun1);
    integerJobSingletonFactory.close();
    assertTrue("Normal type closer works", closerRun1);

  }

  @Test
  public void testNormalType() throws Exception {
    final ClassLoader userCL1 = new ChildFirstClassLoader(new URL[0], JobSingletonInstances.class.getClassLoader(), new String[0], t -> {});
    final RuntimeContext ctx1 = getRuntimeContext("task", userCL1);
    final Integer w0 = JobSingletonFactory.withSupplierAndCloser(() -> 9,null, ctx1, "w0", false).instance();
    final Integer w0_2 = JobSingletonFactory.withSupplierAndCloser(() -> 9, null, ctx1, "w0", false).instance();
    assertSame(w0, w0_2);
    final Double d = 6.7d;
    final Double d0 = JobSingletonFactory.withSupplierAndCloser(() -> d, null, ctx1, "w1", false).instance();
    assertSame(d, d0);
  }
}
