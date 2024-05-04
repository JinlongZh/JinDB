package com.jinlong.jindb.backend.dm;

import com.jinlong.jindb.backend.common.SubArray;
import com.jinlong.jindb.backend.dm.dataItem.DataItem;
import com.jinlong.jindb.backend.dm.dataItem.MockDataItem;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * MockDataManager
 *
 * @Author zjl
 * @Date 2024/5/4
 */
public class MockDataManager implements DataManager {

    private Map<Long, DataItem> cache;
    private Lock lock;

    public static MockDataManager newMockDataManager() {
        MockDataManager dataManager = new MockDataManager();
        dataManager.cache = new HashMap<>();
        dataManager.lock = new ReentrantLock();
        return dataManager;
    }

    @Override
    public DataItem read(long uid) throws Exception {
        lock.lock();
        try {
            return cache.get(uid);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        lock.lock();
        try {
            long uid = 0;
            while (true) {
                uid = Math.abs(new Random(System.nanoTime()).nextInt(Integer.MAX_VALUE));
                if (uid == 0) continue;
                if (cache.containsKey(uid)) continue;
                break;
            }
            DataItem dataItem = MockDataItem.newMockDataItem(uid, new SubArray(data, 0, data.length));
            cache.put(uid, dataItem);
            return uid;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {
    }

}
