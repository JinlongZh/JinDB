package com.jinlong.jindb.backend.common;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * LRU驱逐策略的抽象缓存
 *
 * @Author zjl
 * @Date 2024/5/1
 */
public abstract class AbstractLRUCache<T> {
    // 实际缓存的数据
    private HashMap<Long, T> cacheMap;
    // 表述使用时间的链表
    private LinkedList<Long> cacheKeysList;
    // 正在被某线程获取的资源
    private ConcurrentHashMap<Long, Boolean> gettingCacheMap;

    // 最大缓存资源数
    private int maxResource;
    private Lock lock;

    public AbstractLRUCache(int maxResource) {
        this.maxResource = maxResource;
        cacheMap = new HashMap<>();
        cacheKeysList = new LinkedList<>();
        gettingCacheMap = new ConcurrentHashMap<>();
        lock = new ReentrantLock();
    }

    /**
     * 根据资源key获取资源
     *
     * @param key 资源key
     * @Return T
     */
    protected T get(long key) throws Exception {
        while (true) {
            lock.lock();
            try {
                if (gettingCacheMap.containsKey(key)) {
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
                // 资源在缓存中，直接返回
                if (cacheMap.containsKey(key)) {
                    T obj = cacheMap.get(key);
                    // 将该资源移动到链表头部
                    cacheKeysList.remove(key);
                    cacheKeysList.addFirst(key);
                    return obj;
                }
                // 尝试获取该资源
                gettingCacheMap.put(key, true);
            } finally {
                lock.unlock();
            }
            break;
        }

        // 资源不在缓存中
        T obj = null;
        try {
            obj = getForCache(key);
        } finally {
            lock.lock();
            gettingCacheMap.remove(key);
            lock.unlock();
        }

        lock.lock();
        gettingCacheMap.remove(key);
        // 如果缓存已满，则驱逐链表尾部的资源
        if (cacheMap.size() == maxResource) {
            release(cacheKeysList.getLast());
        }
        cacheMap.put(key, obj);
        cacheKeysList.addFirst(key);
        lock.unlock();

        return obj;
    }

    /**
     * 强行释放一个缓存
     */
    protected void release(long key) {
        lock.lock();
        try {
            T obj = cacheMap.get(key);
            if (obj == null) return;
            releaseForCache(obj);
            cacheMap.remove(key);
            cacheKeysList.remove(key);
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
                release(key);
                cacheMap.remove(key);
                cacheKeysList.remove(key);
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
