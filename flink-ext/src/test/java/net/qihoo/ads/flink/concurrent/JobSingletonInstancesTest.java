package net.qihoo.ads.flink.concurrent;

import java.net.URL;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import org.apache.flink.util.ChildFirstClassLoader;
import org.junit.Test;

import static org.junit.Assert.*;

public class JobSingletonInstancesTest {
  private static final org.slf4j.Logger LOG = net.qihoo.ads.flink.utils.Loggers.get();

  Throwable e = null;
  private final ClassLoader defaultClassLoader = JobSingletonInstances.class.getClassLoader();
  private final ClassLoader userCLu = new ChildFirstClassLoader(new URL[0], defaultClassLoader, new String[0], t -> e = t);
  private final ClassLoader userCLv = new ChildFirstClassLoader(new URL[0], defaultClassLoader, new String[0], t -> e = t);

  @Test
  public void testSingletonForClassLoader() throws Exception {
    final JobSingletonInstances d1 = JobSingletonInstances.getInstance(defaultClassLoader);
    final JobSingletonInstances d2 = JobSingletonInstances.getInstance(defaultClassLoader);
    assertNotNull(d1);
    assertSame(d1, d2);

    final JobSingletonInstances u1 = JobSingletonInstances.getInstance(userCLu);
    final JobSingletonInstances u2 = JobSingletonInstances.getInstance(userCLu);
    assertNotSame(defaultClassLoader, userCLu);
    assertNotNull(u1);
    assertNotSame(d1, u1);
    assertSame(u1, u2);

    final JobSingletonInstances v1 = JobSingletonInstances.getInstance(userCLv);
    final JobSingletonInstances v2 = JobSingletonInstances.getInstance(userCLv);
    assertNotSame(defaultClassLoader, userCLv);
    assertNotSame(userCLu, userCLv);
    assertNotNull(v1);
    assertNotSame(d1, v1);
    assertNotSame(u1, v1);
    assertSame(v1, v2);

    final JobSingletonInstances u3 = JobSingletonInstances.getInstance(userCLu);
    assertNotNull(u3);
    assertSame(u1, u3);

    // no runtime exception
    assertNull(e);
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
  public void testSingletonForKey() throws Exception {
    final JobSingletonInstances manager = JobSingletonInstances.getInstance(defaultClassLoader);
    final AutoCloseable s1 = manager.computeIfAbsent("s", () -> new AutoCloseableWith(null));
    final AutoCloseable s2 = manager.computeIfAbsent("s", () -> new AutoCloseableWith(null));
    final AutoCloseable t1 = manager.computeIfAbsent("t", () -> new AutoCloseableWith(null));
    final AutoCloseable t2 = manager.computeIfAbsent("t", () -> new AutoCloseableWith(null));
    assertNotNull(s1);
    assertNotNull(t1);
    assertSame(s1, s2);
    assertSame(t1, t2);
    assertNotSame(s1, t1);

    final JobSingletonInstances managerU = JobSingletonInstances.getInstance(userCLu);
    final AutoCloseable a1 = managerU.computeIfAbsent("s", () -> new AutoCloseableWith(null));
    final AutoCloseable a2 = managerU.computeIfAbsent("s", () -> new AutoCloseableWith(null));
    final AutoCloseable b1 = managerU.computeIfAbsent("b", () -> new AutoCloseableWith(null));
    final AutoCloseable b2 = managerU.computeIfAbsent("b", () -> new AutoCloseableWith(null));
    assertNotNull(a1);
    assertNotNull(b1);
    assertSame(a1, a2);
    assertSame(b1, b2);
    assertNotSame(a1, b1);
    assertNotSame(a1, s1);
    assertNotSame(a1, t1);
    assertNotSame(b1, s1);
    assertNotSame(b1, t1);
  }

  int releaseCounter = 0;
  ArrayList<String> closedKeys = new ArrayList<String>();

  @Test
  public void testCloseAll() throws Exception {
    final JobSingletonInstances managerV = JobSingletonInstances.getInstance(userCLv);
    assertEquals(0, managerV.resourceStateMap.size());
    assertEquals(0, managerV.orders.size());

    final AutoCloseable v1 = managerV.computeIfAbsent("1", () -> new AutoCloseableWith(c -> { closedKeys.add("1"); ++releaseCounter; }));
    assertEquals(1, managerV.resourceStateMap.size());
    assertEquals(1, managerV.orders.size());

    final AutoCloseable v2 = managerV.computeIfAbsent("1", () -> new AutoCloseableWith(c -> { closedKeys.add("1"); ++releaseCounter; }));
    assertEquals(1, managerV.resourceStateMap.size());
    assertEquals(1, managerV.orders.size());

    final AutoCloseable v3 = managerV.computeIfAbsent("2", () -> new AutoCloseableWith(c -> { closedKeys.add("2"); ++releaseCounter; }));
    assertEquals(2, managerV.resourceStateMap.size());
    assertEquals(2, managerV.orders.size());

    managerV.closeAllByHook();
    assertEquals(0, managerV.resourceStateMap.size());
    assertEquals(0, managerV.orders.size());
    assertEquals(2, releaseCounter);
    assertArrayEquals(new String[]{"2", "1"}, closedKeys.toArray());

    assertNull(managerV.computeIfAbsent("1", () -> new AutoCloseableWith(c -> ++releaseCounter)));
    assertNull(managerV.computeIfAbsent("1", () -> new AutoCloseableWith(c -> ++releaseCounter)));
    assertNull(managerV.computeIfAbsent("2", () -> new AutoCloseableWith(c -> ++releaseCounter)));
    assertNull(managerV.computeIfAbsent("3", () -> new AutoCloseableWith(c -> ++releaseCounter)));
    assertEquals(0, managerV.resourceStateMap.size());
    assertEquals(0, managerV.orders.size());

    managerV.closeAllByHook();
    assertEquals(0, managerV.resourceStateMap.size());
    assertEquals(0, managerV.orders.size());
    assertEquals(2, releaseCounter);
  }

  int unconsistentReleaseCounter = 0;
  volatile AutoCloseableWith unconsistentRef = null;
  @Test
  public void testUnconsistentClean() throws Exception {
    final ClassLoader userCL = new ChildFirstClassLoader(new URL[0], defaultClassLoader, new String[0], t -> e = t);
    final JobSingletonInstances manager = JobSingletonInstances.getInstance(userCL);
    assertNotNull(manager.computeIfAbsent("OK-1", () -> new AutoCloseableWith(null)));
    assertNotNull(manager.computeIfAbsent("OK-2", () -> new AutoCloseableWith(null)));
    assertNotNull(manager.computeIfAbsent("OK-3", () -> new AutoCloseableWith(null)));
    assertEquals(3, manager.resourceStateMap.size());

    final CountDownLatch countDown = new CountDownLatch(2);
    final CountDownLatch supRunning = new CountDownLatch(1);
    // create instance slowly
    new Thread() {
      @Override
      public void run() {
        synchronized (JobSingletonInstancesTest.class) {
          countDown.countDown();
          manager.computeIfAbsent("1", () -> {
            unconsistentRef = new AutoCloseableWith(c -> ++unconsistentReleaseCounter);
            supRunning.countDown();
            try {
              Thread.sleep(800); // slow action
            } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
            }
            return unconsistentRef;
          });
        }
      }
    }.start();

    // release
    Thread r = new Thread() {
      @Override
      public void run() {
        synchronized (manager) {
          countDown.countDown();
          try {
            supRunning.await(); // wait supplier is running
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          manager.closeAllByHook();
        }
      }
    };
    r.start();

    countDown.await(); // wait all thread run
    supRunning.await(); // wait supplier is running
    assertEquals(3, manager.resourceStateMap.size());
    assertEquals(3, manager.orders.size());

    // interrupt closeAll
    Thread.sleep(100);
    r.interrupt();
    synchronized (manager) { // wait closeAllByHook() finish
      assertEquals(0, manager.resourceStateMap.size());
      assertEquals(0, manager.orders.size());
      manager.computeIfAbsent("2", () -> new AutoCloseableWith(null));
    }

    // after value of key "1" is ready
    synchronized (JobSingletonInstancesTest.class) {
      assertNotNull(unconsistentRef);
      assertEquals(0, manager.resourceStateMap.size());
      assertEquals(0, manager.orders.size());
      assertEquals(1, unconsistentReleaseCounter);
    }

    assertNull(manager.computeIfAbsent("3", () -> new AutoCloseableWith(null)));
    assertEquals(0, manager.resourceStateMap.size());
    assertEquals(0, manager.orders.size());
  }
  @Test
  public void testReferenceCountedObject() throws Exception {
    final JobSingletonInstances managerV = JobSingletonInstances.getInstance(userCLv);

    managerV.computeIfAbsent("1", () -> new AutoCloseableWith(c -> { closedKeys.add("1"); ++releaseCounter; }));
    managerV.computeIfAbsent("1", () -> new AutoCloseableWith(c -> { closedKeys.add("1"); ++releaseCounter; }));
    managerV.computeIfAbsent("1", () -> new AutoCloseableWith(c -> { closedKeys.add("1"); ++releaseCounter; }));
    managerV.computeIfAbsent("2", () -> new AutoCloseableWith(c -> { closedKeys.add("2"); ++releaseCounter; }));

    managerV.closeWithName("1");
    assertEquals(2, managerV.resourceStateMap.size());
    assertFalse(managerV.resourceStateMap.get("1").isClosed());
    managerV.closeWithName("1");
    managerV.closeWithName("1");
    assertNull(managerV.resourceStateMap.get("1"));
    assertEquals(1, managerV.resourceStateMap.size());

  }

}
