package com.jinlong.jindb.backend.dm.page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * MockPage
 *
 * @Author zjl
 * @Date 2024/5/1
 */
public class MockPage implements Page {

    private int pageNo;
    private byte[] data;
    private Lock lock = new ReentrantLock();

    public MockPage(int pageNo, byte[] data) {
        this.pageNo = pageNo;
        this.data = data;
    }

    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public void unlock() {
        lock.unlock();
    }

    @Override
    public void release() {}

    @Override
    public void setDirty(boolean dirty) {}

    @Override
    public boolean isDirty() {
        return false;
    }

    @Override
    public int getPageNumber() {
        return pageNo;
    }

    @Override
    public byte[] getData() {
        return data;
    }

}
