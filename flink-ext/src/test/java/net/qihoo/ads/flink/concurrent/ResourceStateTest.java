package net.qihoo.ads.flink.concurrent;

import org.junit.Test;

import java.util.function.Consumer;

import static org.junit.Assert.*;


public class ResourceStateTest {

    private static final org.slf4j.Logger LOG = net.qihoo.ads.flink.utils.Loggers.get();

    int releaseCounter = 0;
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
    public void testCloseAll() {
        ResourceState resourceState = new ResourceState(null, "resource1");
        assertNull(resourceState.getAutoCloseable());

        ResourceState resourceState1 = new ResourceState( new AutoCloseableWith(null), "resource1");
        assertNotNull(resourceState1.getAutoCloseable());

        ResourceState resourceState2 = new ResourceState( new AutoCloseableWith(c -> ++releaseCounter), "resource1");
        resourceState2.reduceResourceReference();
        assertEquals(releaseCounter, 1);
        assertTrue(resourceState2.isClosed());

        // ensure last exit Flink slot release this resource
        ResourceState resourceState3 = new ResourceState( new AutoCloseableWith(null), "resource1");
        resourceState3.addResourceReference();
        resourceState3.reduceResourceReference();
        assertFalse(resourceState3.isClosed());
        resourceState3.reduceResourceReference();
        assertTrue(resourceState3.isClosed());

        // close times > reference times, This will throw AssertionError by ReferenceCountedObject
        boolean isCloseError = false;
        try {
            resourceState3.reduceResourceReference();
        } catch (AssertionError e) {
            LOG.info(e.getMessage());
            isCloseError = true;
        }
        assertTrue(isCloseError);


        // Hook action ignores how many slot reference this resource
        ResourceState resourceState4 = new ResourceState( new AutoCloseableWith(null), "resource1");
        resourceState4.addResourceReference();
        resourceState4.addResourceReference();
        resourceState4.addResourceReference();
        resourceState4.releaseResourceByHook();
        assertTrue(resourceState4.isClosed());


    }
}
