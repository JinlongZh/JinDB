package com.jinlong.jindb.backend.dm.page;

import com.jinlong.jindb.backend.dm.pcache.PageCache;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 页面实现类
 *
 * @Author zjl
 * @Date 2024/5/1
 */
public class PageImpl implements Page {

    private int pageNumber;
    private byte[] data;
    private boolean dirty;
    private Lock lock;

    private PageCache pageCache;

    public PageImpl(int pageNumber, byte[] data, PageCache pageCache) {
        this.pageNumber = pageNumber;
        this.data = data;
        this.pageCache = pageCache;
        lock = new ReentrantLock();
    }

    public void lock() {
        lock.lock();
    }

    public void unlock() {
        lock.unlock();
    }

    public void release() {
        pageCache.release(this);
    }

    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    public boolean isDirty() {
        return dirty;
    }

    public int getPageNumber() {
        return pageNumber;
    }

    public byte[] getData() {
        return data;
    }
}
