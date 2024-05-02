package com.jinlong.jindb.backend.dm.dataItem;

import com.jinlong.jindb.backend.common.SubArray;
import com.jinlong.jindb.backend.dm.DataManagerImpl;
import com.jinlong.jindb.backend.dm.page.Page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * dataItem 结构如下：
 * [ValidFlag] [DataSize] [Data]
 * ValidFlag 1字节，0为合法，1为非法
 * DataSize  2字节，标识Data的长度
 *
 * @Author zjl
 * @Date 2024/5/2
 */
public class DataItemImpl implements DataItem {

    static final int OF_VALID = 0;
    static final int OF_SIZE = 1;
    static final int OF_DATA = 3;

    private SubArray raw;
    private byte[] oldRaw;
    private Lock rLock;
    private Lock wLock;
    private DataManagerImpl dataManager;
    private long uid;
    private Page page;

    public DataItemImpl(SubArray raw, byte[] oldRaw, Page page, long uid, DataManagerImpl dataManager) {
        this.raw = raw;
        this.oldRaw = oldRaw;
        ReadWriteLock lock = new ReentrantReadWriteLock();
        rLock = lock.readLock();
        wLock = lock.writeLock();
        this.dataManager = dataManager;
        this.uid = uid;
        this.page = page;
    }

    public boolean isValid() {
        return raw.raw[raw.start + OF_VALID] == (byte) 0;
    }

    @Override
    public SubArray data() {
        return new SubArray(raw.raw, raw.start + OF_DATA, raw.end);
    }

    @Override
    public void before() {
        wLock.lock();
        page.setDirty(true);
        System.arraycopy(raw.raw, raw.start, oldRaw, 0, oldRaw.length);
    }

    @Override
    public void unBefore() {
        System.arraycopy(oldRaw, 0, raw.raw, raw.start, oldRaw.length);
        wLock.unlock();
    }

    @Override
    public void after(long xid) {
        dataManager.logDataItem(xid, this);
        wLock.unlock();
    }

    @Override
    public void release() {
        dataManager.releaseDataItem(this);
    }

    @Override
    public void lock() {
        wLock.lock();
    }

    @Override
    public void unlock() {
        wLock.unlock();
    }

    @Override
    public void rLock() {
        rLock.lock();
    }

    @Override
    public void rUnLock() {
        rLock.unlock();
    }

}
