package net.qihoo.ads.flink.concurrent;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.shaded.guava31.com.google.common.hash.Hashing;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;
/**
 * A Factory for creating and managing the flink job level singletons for sharing objects within a flink job crossing
 * the different operator slots and different operators.
 *
 * <p>A job singleton is bound the to a flink user code class loader, and automatically released at the release hook
 * running stage when the job is closing its specific class loader. The release order is the reverse order of creating.
 *
 * <p>Example:
 *
 * <pre>
 *   class MyOperator implements RichFunction {
 *
 *     JobSingletonFactory jobInstance;
 *
 *     Supplier<ThreadPoolExecutor> supplier = () -> "create threadPool method";
 *     Consumer<ThreadPoolExecutor> consumer = () -> "close threadPool method";
 *
 *     Supplier<SqlSessionFactory> dbSupplier = () -> "create sqlSession method";
 *     Consumer<SqlSessionFactory> dbConsumer = () -> "close  sqlSession method";
 *
 *     @Overide
 *     void open(Configuration parameters) throws Exception {
 *        // declare ThreadPoolExecutor
 *       jobInstance = JobSingletonFactory.withSupplierAndCloser(supplier, consumer,  getRuntimeContext(), "thread key", false);
 *       ThreadPoolExecutor threadPool = jobInstance.instance();
 *       // declare SqlSessionFactory
 *       dbJobInstance = JobSingletonFactory.withSupplierAndCloser(dbSupplier, dbConsumer,  getRuntimeContext(), "mybatis key", false);
 *       SqlSessionFactory sqlFactory = dbJobInstance.instance();
 *
 *       // Using ThreadPool execute db insert action
 *       threadPool.submit(new Thread(() -> sqlFactory.openSession().insert("....)));
 *     }
 *
 *
 *     @Overide
 *     void close() throws Exception {
 *       // we should close by reverse order manually, otherwise it could cause resource leakage or close failed.
 *       dbJobInstance.close();
 *       jobInstance.close();
 *     }
 *     // when TM classloader was released, closeAllByHook will ensure reverse order closing action executed opposite to declare order.
 *
 *   }
 * </pre>
 */
public class JobSingletonFactory<T> {

  private static final org.slf4j.Logger LOG = net.qihoo.ads.flink.utils.Loggers.get();
  private final JobSingletonInstances instances;
  private final Supplier<T> suppliper;
  private Consumer<T> closer;

  private final RuntimeContext runtimeContext;
  private final String resourceName;

  private JobSingletonFactory(JobSingletonInstances instances,
                              Supplier<T> suppliper,
                              Consumer<T> closer,
                              RuntimeContext runtimeContext,
                              String resourceName) {
    this.suppliper = suppliper;
    this.instances = instances;
    this.closer = closer;
    this.runtimeContext = runtimeContext;
    this.resourceName = resourceName;
  }

  public static <T> JobSingletonFactory<T> withSupplierAndCloser(final Supplier<T> supplier,
                                                                 final Consumer<T> closer,
                                                                 RuntimeContext runtimeContext,
                                                                 String name,
                                                                 boolean opScope) throws Exception {
    JobSingletonInstances instances = Objects.requireNonNull(JobSingletonInstances.getInstance(Objects.requireNonNull(runtimeContext).getUserCodeClassLoader()));
    String uniqueName;
    String prefix;
    if (opScope) {
      final String taskName = runtimeContext.getTaskName();
      final int len = taskName.length();
      if (len < 16) {
        prefix = "operator$"
                + runtimeContext.getNumberOfParallelSubtasks() + "$"
                + len + "$"
                + taskName + "$";
      } else {
        prefix = "operator$"
                + runtimeContext.getNumberOfParallelSubtasks() + "$"
                + len + "$"
                + Hashing.murmur3_128().hashString(taskName, StandardCharsets.UTF_8) + "$"
                + taskName.substring(0, 16) + "$";
      }
    } else {
      prefix = "job$";
    }
    uniqueName = prefix + Objects.requireNonNull(name);
    return new JobSingletonFactory<>(instances, supplier, closer, runtimeContext, uniqueName);
  }

  public static <T> JobSingletonFactory<T> withSupplier(final Supplier<T> supplier,
                                                                  RuntimeContext runtimeContext,
                                                                  String name,
                                                                  boolean opScope) throws Exception {

    return withSupplierAndCloser(supplier, null, runtimeContext, name, opScope);
  }

  // the returned reference can be safely used before operator.close() finished
  public T instance() {

    final AutoCloseable w = instances.computeIfAbsent(resourceName, () -> {
      LOG.info("Creating the job singleton with key [{}]", resourceName);

      final T t = Objects.requireNonNull(suppliper.get(), "Failed to create an instance.");

      // All { key -> singleton } will be released in JobSingletonInstances.closeAll() in the reverse order,
      // so the key of registerUserCodeClassLoaderReleaseHookIfAbsent() is always the same one.
      runtimeContext.registerUserCodeClassLoaderReleaseHookIfAbsent(
          "Release singletons created by JobSingletonFactory for job " + runtimeContext.getJobId(),
          instances::closeAllByHook);
      if (t instanceof AutoCloseable) {
        return (AutoCloseable)t;
      } else {
        return new AutoCloseableWrapper<>(t, closer);
      }
    });

    if (w instanceof AutoCloseableWrapper) {
      @SuppressWarnings("unchecked")
      final AutoCloseableWrapper<T> wrapper = (AutoCloseableWrapper<T>)w;
      return wrapper.o;
    } else {
      @SuppressWarnings("unchecked")
      final T result = (T)w;
      return result;
    }
  }
  public void close() {
    // "instances" stored all resource declared by different withSupplierAndCloser,
    // Here we set resourceName to ensure that who generate, who release.
    this.instances.closeWithName(resourceName);
  }

  private static class AutoCloseableWrapper<T> implements AutoCloseable {
    final T o;
    private final Consumer<T> closer;
    AutoCloseableWrapper(final T o, Consumer<T> closer) {
      this.o = o;
      this.closer = closer;
    }
    @Override
    public void close() {
      if (null != closer && null != o) {
        closer.accept(o);
      }
    }
  }
}
