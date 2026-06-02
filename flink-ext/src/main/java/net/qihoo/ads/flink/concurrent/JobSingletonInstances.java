package net.qihoo.ads.flink.concurrent;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.shaded.guava31.com.google.common.io.ByteStreams;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Deque;
import java.util.Iterator;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Supplier;


class JobSingletonInstances {
  private static final org.slf4j.Logger LOG = net.qihoo.ads.flink.utils.Loggers.get();
  private static final JobSingletonInstances FOR_CURRENT_CLASSLOADER = new JobSingletonInstances();

  // A stub class loaded by the flink user code class loader.
  // This class and its static fields will be defined via reflection
  // by a flink job user code class loader to action as a job level singleton.
  private static final class Holder { static volatile Object O; }
  private static final byte[] HOLDER_BYTES;
  private static final Method GET_CLASS_LOADING_LOCK;
  private static final Method FIND_LOADED_CLASS;
  private static final Method DEFINE_CLASS;
  private static final Throwable REFLECT_EXCEPTION;

  static {
    final String classPath = Holder.class.getName().replace('.', '/') + ".class";
    byte[] bytes = null;
    Method getClassLoadingLock = null;
    Method findLoadedClass = null;
    Method defineClass = null;
    Throwable t = null;

    try (final InputStream in = Holder.class.getClassLoader().getResourceAsStream(classPath)) {
      // load class bytes for Holder class
      bytes = ByteStreams.toByteArray(in);

      // reflect api
      getClassLoadingLock = ClassLoader.class.getDeclaredMethod("getClassLoadingLock", String.class);
      findLoadedClass = ClassLoader.class.getDeclaredMethod("findLoadedClass", String.class);
      defineClass = ClassLoader.class.getDeclaredMethod("defineClass", String.class, byte[].class, int.class, int.class);

      getClassLoadingLock.setAccessible(true);
      findLoadedClass.setAccessible(true);
      defineClass.setAccessible(true);
    } catch (final Throwable e) {
      t = e;
    }

    HOLDER_BYTES = bytes;
    GET_CLASS_LOADING_LOCK = getClassLoadingLock;
    FIND_LOADED_CLASS = findLoadedClass;
    DEFINE_CLASS = defineClass;
    REFLECT_EXCEPTION = t;
  }

  // fields for managing singletons by name
  @VisibleForTesting
  final Deque<String> orders = new LinkedBlockingDeque<String>();
  @VisibleForTesting final ConcurrentMap<String, ResourceState> resourceStateMap = new ConcurrentHashMap<String, ResourceState>();
  private final AtomicBoolean closed = new AtomicBoolean(false);
  private final ReadWriteLock rwlock = new ReentrantReadWriteLock();

  // this method should be called by Flink open()
  static JobSingletonInstances getInstance(final ClassLoader targetClassLoader) throws Exception {
    if (JobSingletonInstances.class.getClassLoader() == Objects.requireNonNull(targetClassLoader)) {
      return FOR_CURRENT_CLASSLOADER;
    }

    // via refection
    final String className = Holder.class.getName();

    Class<?> clz = null;
    synchronized (GET_CLASS_LOADING_LOCK.invoke(targetClassLoader, className)) {
      clz = (Class<?>)FIND_LOADED_CLASS.invoke(targetClassLoader, className);
      if (null == clz) {
        if (null != REFLECT_EXCEPTION) {
          throw new RuntimeException(REFLECT_EXCEPTION);
        }

        // Load the Holder class from bytes directly by the targetClassLoader.
        // The Holder class is no need to be resolved, since it has no dependency.
        clz = (Class<?>)DEFINE_CLASS.invoke(targetClassLoader, className, HOLDER_BYTES, 0, HOLDER_BYTES.length);

        // initialize
        final Field f = clz.getDeclaredField("O");
        f.setAccessible(true);
        f.set(null, new JobSingletonInstances());
      }
    }

    final Field f = clz.getDeclaredField("O");
    f.setAccessible(true);
    return (JobSingletonInstances)f.get(null);
  }

  AutoCloseable computeIfAbsent(final String key, final Supplier<AutoCloseable> generator) {
    Objects.requireNonNull(key);
    Objects.requireNonNull(generator);
    final Lock rlock = rwlock.readLock();
    if (rlock.tryLock()) {
      try {
        if (closed.get()) {
          return null; // already closed by closeAllByHook
        } else {
          // register reference && closed && orders for this resource key
          ResourceState computeResult = resourceStateMap.compute(key, (s, resourceState) -> {
            if (resourceState == null) {
              final AutoCloseable result = generator.get();
              orders.add(s);
              return new ResourceState(result, key);
            }
            if (resourceState.isClosed()) {
              return resourceState;
            }
            return resourceState.addResourceReference();
          });
          return computeResult.getAutoCloseable();
        }
      } catch (Throwable e) {
        LOG.error("Failed to create an instance.", e);
        try {
          resourceStateMap.computeIfPresent(key, (k, v) -> v.isClosed() ? v : v.reduceResourceReference());
        } catch (AssertionError error) {
          LOG.error("Rollback to create an instance error! ", error);
        }
      } finally {
        rlock.unlock();
      }
    } else {
      return null; //closeAllByHook() is running
    }
    return null;
  }

  // This function should run after all operator.close() finish
  // After this function finished, computeIfAbsent() will always return null
  void closeAllByHook() {
    final Lock wlock = rwlock.writeLock();
    try {
      wlock.lock();
      for (final Iterator<String> it = orders.descendingIterator(); it.hasNext(); ) {
        final String resourceName = it.next();
        resourceStateMap.computeIfPresent(resourceName, (k,v) -> v.releaseResourceByHook());
        it.remove();
      }
    } catch (AssertionError error) {
      LOG.error("Close by Flink Hook failed! ", error);
    } finally {
      orders.clear();
      resourceStateMap.clear();
      closed.getAndSet(true);
      wlock.unlock();
    }
  }

  // This function ensure last exit slot to release resource safely in Flink.
  void closeWithName(String resourceName) {
    final Lock rlock = rwlock.readLock();
    try {
      rlock.lock();
      resourceStateMap.computeIfPresent(resourceName, (k,v) -> {
        // remove resource key If this has been closed
        if (v.isClosed()) {
          return null;
        } else {
          // try close
          v.reduceResourceReference();
          if (v.isClosed()) {
            return null;
          } else {
            return v;
          }
        }
      });
    } catch (AssertionError error) {
      LOG.error("Close with name failed! ", error);
    } finally {
      rlock.unlock();
    }
  }
}
