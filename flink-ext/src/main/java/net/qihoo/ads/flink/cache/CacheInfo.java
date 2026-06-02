package net.qihoo.ads.flink.cache;

import com.github.benmanes.caffeine.cache.stats.CacheStats;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author zhuming
 * @date 2022/6/7 14:31
 */
class CacheInfo {
    private long cacheSize;
    private long cacheActualSize;
    /**
     * hitCount;  // 缓存命中数
     * missCount; // 缓存未命中数
     * loadSuccessCount; // load成功数
     * loadExceptionCount; // load异常数
     * totalLoadTime; // load的总共耗时
     * evictionCount; // 缓存项被回收的总数,不包括显式清除
     */
     private CacheStats cacheStats;

    /**
     RemovalCause.REPLACED
     RemovalCause.EXPIRED
     RemovalCause.SIZE
     RemovalCause.REPLACED
     */
    private  HashMap<String, AtomicInteger>  cacheEvictInfo;

    public CacheInfo(long cacheSize, long cacheActualSize, CacheStats cacheStats, HashMap<String, AtomicInteger> cacheEvictInfo) {
        this.cacheSize = cacheSize;
        this.cacheActualSize = cacheActualSize;
        this.cacheStats = cacheStats;
        this.cacheEvictInfo = cacheEvictInfo;
    }

    public CacheInfo() {
        this(0,  0, CacheStats.empty(), new HashMap<>());
    }

    public long getCacheSize() {
        return cacheSize;
    }

    public HashMap<String, AtomicInteger> getCacheEvictInfo() {
        return cacheEvictInfo;
    }

    public void setCacheSize(long cacheSize) {
        this.cacheSize = cacheSize;
    }

    public long getCacheActualSize() {
        return cacheActualSize;
    }

    public void setCacheActualSize(int cacheActualSize) {
        this.cacheActualSize = cacheActualSize;
    }

    public CacheStats getCacheStats() {
        return cacheStats;
    }

    public void setCacheStats(CacheStats cacheStats) {
        this.cacheStats = cacheStats;
    }

    public void setCacheEvictInfo(HashMap<String, AtomicInteger> cacheEvictInfo) {
        this.cacheEvictInfo = cacheEvictInfo;
    }
}