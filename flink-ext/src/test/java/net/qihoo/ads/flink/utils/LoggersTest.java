package net.qihoo.ads.flink.utils;

import org.junit.Test;
import org.slf4j.Logger;
import static org.junit.Assert.*;
import static org.slf4j.LoggerFactory.getLogger;

/**
 * Test {@link Loggers}
 */
public class LoggersTest {
  private static final Logger log1 = Loggers.get();
  private static final Logger log2 = getLogger(LoggersTest.class);

  @Test
  public void checkLogger() throws Exception {
    // verify log4j impl is correct
    assertSame(Loggers.get("aaa"), Loggers.get("aaa"));
    assertNotSame(Loggers.get("aaa"), Loggers.get("bbb"));

    assertSame(log1, log2);
    assertSame(log1, getLogger(LoggersTest.class.getName()));
  }

  private static class InnerClass {
    private static final Logger log3 = Loggers.get();
    private static final Logger log4 = Loggers.get(InnerClass.class);
    private static final Logger log5 = getLogger(InnerClass.class.getName());
  }

  @Test
  public void checkInnerLogger() throws Exception {
    assertSame(InnerClass.log3, InnerClass.log4);
    assertSame(InnerClass.log3, InnerClass.log5);
    assertSame(InnerClass.log3, Loggers.get("net.qihoo.ads.flink.utils.LoggersTest$InnerClass"));
  }

  @Test
  public void checkTempClass() throws Exception {
    Logger log6 = new InnerClass() {
      public Logger log6 = Loggers.get();
    }.log6;

    assertNotSame(log6, InnerClass.log3);
    assertSame(log6, Loggers.get("net.qihoo.ads.flink.utils.LoggersTest$1"));
  }
}
