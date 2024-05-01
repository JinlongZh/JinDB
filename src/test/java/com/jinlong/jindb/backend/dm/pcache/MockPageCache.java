package com.jinlong.jindb.backend.dm.pcache;

import com.jinlong.jindb.backend.dm.page.MockPage;
import com.jinlong.jindb.backend.dm.page.Page;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * MockPageCache
 *
 * @Author zjl
 * @Date 2024/5/1
 */
public class MockPageCache implements PageCache {

    private Map<Integer, MockPage> cacheMap = new HashMap<>();
    private Lock lock = new ReentrantLock();
    private AtomicInteger noPages = new AtomicInteger(0);

    @Override
    public int newPage(byte[] initData) {
        lock.lock();
        try {
            int pageNo = noPages.incrementAndGet();
            MockPage page = new MockPage(pageNo, initData);
            cacheMap.put(pageNo, page);
            return pageNo;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Page getPage(int pageNo) throws Exception {
        lock.lock();
        try {
            return cacheMap.get(pageNo);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void close() {}

    @Override
    public void release(Page page) {}

    @Override
    public void truncateByPageNo(int maxPageNo) {}

    @Override
    public int getPageNumber() {
        return noPages.intValue();
    }

    @Override
    public void flushPage(Page pg) {}

}
