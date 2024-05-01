package com.jinlong.jindb.backend.common;

import com.jinlong.jindb.common.ErrorConstants;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 引用计数缓存淘汰策略抽象缓存
 *
 * @Author zjl
 * @Date 2024/5/1
 */
public abstract class AbstractRefCountCache<T> {
    // 实际缓存的数据
    private HashMap<Long, T> cacheMap;
    // 资源的引用个数
    private HashMap<Long, Integer> referenceCountMap;
    // 正在被某线程获取的资源
    private HashMap<Long, Boolean> gettingCacheMap;

    // 缓存的最大缓存资源数
    private int maxResource;
    // 缓存中元素的个数
    private int count = 0;
    private Lock lock;

    public AbstractRefCountCache(int maxResource) {
        this.maxResource = maxResource;
        cacheMap = new HashMap<>();
        referenceCountMap = new HashMap<>();
        gettingCacheMap = new HashMap<>();
        lock = new ReentrantLock();
    }

    /**
     * 根据资源key获取资源
     *
     * @param key 资源key
     * @Return T
     */
    protected T get(long key) throws Exception {
        while(true) {
            lock.lock();
            if(gettingCacheMap.containsKey(key)) {
                // 请求的资源正在被其他线程获取
                lock.unlock();
                try {
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                continue;
            }

            if(cacheMap.containsKey(key)) {
                // 资源在缓存中，直接返回
                T obj = cacheMap.get(key);
                referenceCountMap.put(key, referenceCountMap.get(key) + 1);
                lock.unlock();
                return obj;
            }

            // 尝试获取该资源
            if(maxResource > 0 && count == maxResource) {
                lock.unlock();
                throw ErrorConstants.CacheFullException;
            }
            gettingCacheMap.put(key, true);
            lock.unlock();
            break;
        }

        // 资源不在缓存中
        T obj = null;
        try {
            obj = getForCache(key);
        } catch (Exception e) {
            lock.lock();
            gettingCacheMap.remove(key);
            lock.unlock();
            throw e;
        }

        lock.lock();
        count++;
        cacheMap.put(key, obj);
        referenceCountMap.put(key, 1);
        gettingCacheMap.remove(key);
        lock.unlock();

        return obj;
    }

    /**
     * 强行释放一个缓存
     */
    protected void release(long key) {
        lock.lock();
        try {
            int ref = referenceCountMap.get(key) - 1;
            if (ref == 0) {
                T obj = cacheMap.get(key);
                releaseForCache(obj);
                referenceCountMap.remove(key);
                cacheMap.remove(key);
                count--;
            } else {
                referenceCountMap.put(key, ref);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 关闭缓存，写回所有资源
     */
    protected void close() {
        lock.lock();
        try {
            Set<Long> keys = cacheMap.keySet();
            for (long key : keys) {
                T obj = cacheMap.get(key);
                releaseForCache(obj);
                referenceCountMap.remove(key);
                cacheMap.remove(key);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 当资源不在缓存时的获取行为
     */
    protected abstract T getForCache(long key) throws Exception;

    /**
     * 当资源被驱逐时的写回行为
     */
    protected abstract void releaseForCache(T obj);

}
