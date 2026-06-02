package net.qihoo.ads.flink.concurrent;

import net.almson.object.ReferenceCountedObject;

/**
 * Static Resource Management in Flink...other similar frame.
 * 1. closed: this represents that whether this static resource has been released.
 * 2. autoCloseable: this package the releasing action like these.
 *       clear(),close(),clean().. action in HashMap, ArrayList, Set...Cache, Connection, Socket...
 * 3. referenceCount: this represents how many tasks reference this resource.
 *       If we set 8 slots in TaskManager, when all open() function have been executed, referenceCount.get==7.
 *       when all close() executed, we just let last exit slot which referenceCount.get==0 release resource
 *
 */
public class ResourceState extends ReferenceCountedObject {
    private static final org.slf4j.Logger LOG = net.qihoo.ads.flink.utils.Loggers.get();
    private volatile AutoCloseable autoCloseable;

    private final String resourceName;

    public ResourceState(AutoCloseable autoCloseable, String resourceName) {
        this.autoCloseable = autoCloseable;
        this.resourceName = resourceName;
    }

    public boolean isClosed() {
        return autoCloseable == null;
    }

    public ResourceState addResourceReference() {
        // getAndIncrement
        this.retain();
        return this;
    }

    public ResourceState reduceResourceReference() {
        // decrementAndGet
        this.release();
        return this;
    }

    public AutoCloseable getAutoCloseable() {
        return this.autoCloseable;
    }

    public ResourceState releaseResourceByHook() {
        destroy();
        return this;
    }

    @Override
    protected void destroy() {
        if (!isClosed()) {
            try (final AutoCloseable c = autoCloseable) {
                autoCloseable = null;
                LOG.info("Releasing the job shared resource with key [{}] by destroy", resourceName);
            } catch (final Throwable e) {
                LOG.error("Fail to release the job shared resource with key [{}] by destroy", resourceName, e);
            }
        }
        else {
            LOG.info("no matter which judge condition, it indicates this resourceName is already closed");
        }
    }
}
