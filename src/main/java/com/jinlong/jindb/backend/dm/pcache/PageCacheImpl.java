package com.jinlong.jindb.backend.dm.pcache;

import com.jinlong.jindb.backend.common.AbstractRefCountCache;
import com.jinlong.jindb.backend.dm.page.Page;
import com.jinlong.jindb.backend.dm.page.PageImpl;
import com.jinlong.jindb.backend.utils.Panic;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 页面缓存实现类
 *
 * @Author zjl
 * @Date 2024/5/1
 */
public class PageCacheImpl extends AbstractRefCountCache<Page> implements PageCache {

    // 内存最小限制
    private static final int MEMORY_MIN_LIMIT = 10;
    public static final String DB_SUFFIX = ".db";

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock fileLock;

    private AtomicInteger pageNumbers;

    public PageCacheImpl(int maxResource, RandomAccessFile file, FileChannel fileChannel) {
        super(maxResource);
        if (maxResource < MEMORY_MIN_LIMIT) {
            Panic.panic(new RuntimeException("Memory too small"));
        }
        long length = 0;
        try {
            length = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.file = file;
        this.fc = fileChannel;
        this.fileLock = new ReentrantLock();
        this.pageNumbers = new AtomicInteger((int) length / PAGE_SIZE);
    }

    @Override
    public int newPage(byte[] initData) {
        int pageNo = pageNumbers.incrementAndGet();
        Page page = new PageImpl(pageNo, initData, null);
        flush(page);
        return pageNo;
    }

    @Override
    public Page getPage(int pageNo) throws Exception {
        return get((long) pageNo);
    }

    @Override
    protected Page getForCache(long key) throws Exception {
        int pageNo = (int) key;
        long offset = PageCacheImpl.pageOffset(pageNo);

        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);

        fileLock.lock();
        try {
            fc.position(offset);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        fileLock.unlock();

        return new PageImpl(pageNo, buf.array(), this);
    }

    @Override
    protected void releaseForCache(Page page) {
        if (page.isDirty()) {
            flush(page);
            page.setDirty(false);
        }
    }

    @Override
    public void release(Page page) {
        release((long) page.getPageNumber());
    }

    @Override
    public void truncateByPageNo(int maxPageNo) {
        long size = pageOffset(maxPageNo + 1);
        try {
            file.setLength(size);
        } catch (IOException e) {
            Panic.panic(e);
        }
        pageNumbers.set(maxPageNo);
    }

    @Override
    public int getPageNumber() {
        return pageNumbers.intValue();
    }

    @Override
    public void flushPage(Page page) {
        flush(page);
    }

    @Override
    public void close() {
        super.close();
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    private static long pageOffset(int pageNo) {
        return (long) (pageNo - 1) * PAGE_SIZE;
    }

    private void flush(Page page) {
        int pageNo = page.getPageNumber();
        long offset = pageOffset(pageNo);

        fileLock.lock();
        try {
            ByteBuffer buf = ByteBuffer.wrap(page.getData());
            fc.position(offset);
            fc.write(buf);
            fc.force(true);
        } catch (IOException e) {
            Panic.panic(e);
        } finally {
            fileLock.unlock();
        }
    }
}
