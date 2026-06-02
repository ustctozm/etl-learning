package net.qihoo.ads.flink.utils;

import org.slf4j.Logger;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * The <code>Loggers</code> is a utility class delegating <code>org.slf4j.LoggerFactory</code> class,
 * and add an API <code>Loggers.get()</code> for getting the default logger using caller's class name.
 *
 * Usage:
 *
 * <pre>
 *   class MyClass {
 *     private static final org.slf4j.Logger LOG = net.qihoo.ads.flink.utils.Loggers.get();
 *   }
 * </pre>
 *
 * Ref:
 * [0]: https://github.com/apache/flink/blob/master/docs/content.zh/docs/deployment/advanced/logging.md
 * [1]: https://adgit.src.corp.qihoo.net/casino/mv-common/-/blob/master/src/main/java/com/mediav/utils/Loggers.java
 */
public final class Loggers extends SecurityManager
{
  private Loggers() {}

  private static final class Singleton {
    private static final Loggers INSTANCE = new Loggers();
  }

  /**
   * Get the default logger using caller's class name.
   *
   * Usage:
   *
   * <pre>
   *   class MyClass {
   *     private static final org.slf4j.Logger LOG = net.qihoo.ads.flink.utils.Loggers.get();
   *   }
   * </pre>
   */
  public static Logger get()
  {
    return getLogger(Singleton.INSTANCE.getClassContext()[1]);
  }

  /**
   * Delegate to org.slf4j.LoggerFactory.getLogger(String name)
   */
  public static Logger get(final String name) {
    return getLogger(name);
  }

  /**
   * Delegate to org.slf4j.LoggerFactory.getLogger(Class clazz)
   */
  public static Logger get(final Class clazz) {
    return getLogger(clazz);
  }
}
