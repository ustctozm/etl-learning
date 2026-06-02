package net.qihoo.ads.flink.cache;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.CacheWriter;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.stats.CacheStats;

import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class JobCache<K, ACC, OUT> {

    private static final ConcurrentMap<String, ConcurrentMap> aggCacheForOperator = new ConcurrentHashMap<>();
    private static final ConcurrentMap<String, ConcurrentMap> evictBufferForOperator = new ConcurrentHashMap<>();

    private final ConcurrentMap<Long, Cache<K, ACC>> aggCachePerCP;
    private final ConcurrentMap<Long, ConcurrentLinkedQueue<OUT>> evictBufferPerCP;

    private final CacheInfo cacheInfo = new CacheInfo();
    private static final ConcurrentMap<String, HashMap<String, AtomicInteger>> cacheEvictInfoForOperator = new ConcurrentHashMap<>();
    private final HashMap<String, AtomicInteger> cacheEvictInfo;
    private final long cacheSize;
    private final long evictTime;

    @SuppressWarnings("unchecked")
    private JobCache(long cacheSize, long evictTime, String operatorName) {
        //bind local cache for this operatorName
        aggCachePerCP = aggCacheForOperator.computeIfAbsent(operatorName,  k -> new ConcurrentHashMap<>());
        evictBufferPerCP = evictBufferForOperator.computeIfAbsent(operatorName,  k ->new ConcurrentHashMap<>());
        cacheEvictInfo = cacheEvictInfoForOperator.computeIfAbsent(operatorName, k -> new HashMap<>());
        this.cacheSize = cacheSize;
        this.evictTime = evictTime;
    }

    private JobCache(long cacheSize, long evictTime) {
        aggCachePerCP = new ConcurrentHashMap<>();
        evictBufferPerCP = new ConcurrentHashMap<>();
        cacheEvictInfo = new HashMap<>();
        this.cacheSize = cacheSize;
        this.evictTime = evictTime;
    }

    public void cacheContentToEvictBuffer(long checkpointId) {
        Cache<K, ACC> cache =  aggCachePerCP.get(checkpointId);
        if (cache != null) {
            cache.invalidateAll();
        }
    }

    public ConcurrentLinkedQueue<OUT> getEvictBuffer(long checkpointId) {
        return evictBufferPerCP.computeIfAbsent(checkpointId, k -> new ConcurrentLinkedQueue<>());
    }


    public void removeCache(long checkpointId) {
        aggCachePerCP.keySet().removeIf(k -> k <= checkpointId);
        evictBufferPerCP.keySet().removeIf(k -> k <= checkpointId);
        cacheEvictInfo.keySet().clear();
    }

    public CacheInfo getCacheInfo(long checkpointId) {
        int cacheSize = -1;
        CacheStats cacheStats = CacheStats.empty();
        if (aggCachePerCP.get(checkpointId) != null) {
            cacheSize = (int)aggCachePerCP.get(checkpointId).estimatedSize();
            cacheStats = aggCachePerCP.get(checkpointId).stats();
        }
        cacheInfo.setCacheSize(cacheSize);
        cacheInfo.setCacheActualSize(cacheSize);
        cacheInfo.setCacheStats(cacheStats);
        cacheInfo.setCacheEvictInfo(cacheEvictInfo);
        return cacheInfo;
    }

    public ConcurrentMap<K,ACC> getCache(long checkpointId, Function<ACC, OUT> generateOutput) {
        //get record out caffeine cache
        ConcurrentLinkedQueue<OUT> evictBuffer = getEvictBuffer(checkpointId);
        //return caffeine cache
        return aggCachePerCP.computeIfAbsent(checkpointId, (id ->
                Caffeine.newBuilder().maximumSize(cacheSize).
                        recordStats().
                        expireAfterWrite(evictTime, TimeUnit.MILLISECONDS).
                        writer(new CacheWriter<K, ACC>() {
                            @Override
                            public void write(K key, ACC value) {}
                            @Override
                            public void delete(K key, ACC value, RemovalCause cause) {
                                // 过期evictTime后淘汰
                                if (cause == RemovalCause.EXPIRED) {
                                    cacheEvictInfo.computeIfAbsent("expired", k -> new AtomicInteger()).incrementAndGet();
                                }
                                // 超过cacheSize后淘汰
                                else if (cause == RemovalCause.SIZE) {
                                    cacheEvictInfo.computeIfAbsent("size", k -> new AtomicInteger()).incrementAndGet();
                                }
                                // 手动remove数据，对应cp前一次淘汰完cache数据
                                else if (cause == RemovalCause.EXPLICIT) {
                                    cacheEvictInfo.computeIfAbsent("explicit", k -> new AtomicInteger()).incrementAndGet();
                                }
                                // 软引用被GC淘汰
                                else if (cause == RemovalCause.COLLECTED) {
                                    cacheEvictInfo.computeIfAbsent("collected", k -> new AtomicInteger()).incrementAndGet();
                                }
                                // merge后更新value
                                else if (cause == RemovalCause.REPLACED) {
                                    cacheEvictInfo.computeIfAbsent("replaced", k -> new AtomicInteger()).incrementAndGet();
                                }
                                // cp阶段手动清除 ｜｜ 自身淘汰
                                if (cause == RemovalCause.EXPLICIT || cause.wasEvicted()) {
                                    //get record out caffeine cache
                                    evictBuffer.offer(generateOutput.apply(value));
                                }
                            }
                        }).build())).asMap();
    }

    static public <K, ACC, OUT> JobCache<K, ACC, OUT> getCacheInstance(long cacheSize, long evictTime) {
        return new JobCache<>(cacheSize, evictTime);
    }

    static public <K, ACC, OUT> JobCache<K, ACC, OUT> getSharedCacheInstance(long cacheSize, long evictTime,String operatorName) {
        return new JobCache<>(cacheSize, evictTime, operatorName);
    }


}
