package com.jinlong.jindb.backend.dm.pcache;

import com.jinlong.jindb.backend.dm.page.Page;
import com.jinlong.jindb.backend.utils.Panic;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

/**
 * 页面缓存接口
 *
 * @Author zjl
 * @Date 2024/5/1
 */
public interface PageCache {

    // 页面大小：8k
    int PAGE_SIZE = 1 << 13;

    int newPage(byte[] initData);

    Page getPage(int pageNo) throws Exception;

    void close();

    void release(Page page);

    void truncateByPageNo(int maxPageNo);

    int getPageNumber();

    void flushPage(Page page);

    static PageCacheImpl create(String path, long memory) {
        String fileName = path + PageCacheImpl.DB_SUFFIX;
        File file = new File(fileName);
        try {
            if (!file.createNewFile()) {
                Panic.panic(new RuntimeException("File already exists: " + fileName));
            }
        } catch (Exception e) {
            Panic.panic(e);
        }
        if (!file.canRead() || !file.canWrite()) {
            Panic.panic(new RuntimeException("File cannot read or write: " + fileName));
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(file, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new PageCacheImpl((int) memory / PAGE_SIZE, raf, fc);
    }

    static PageCacheImpl open(String path, long memory) {
        String fileName = path + PageCacheImpl.DB_SUFFIX;
        File file = new File(fileName);
        if (!file.exists()) {
            Panic.panic(new RuntimeException("File does not exists: " + fileName));
        }
        if (!file.canRead() || !file.canWrite()) {
            Panic.panic(new RuntimeException("File cannot read or write: " + fileName));
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(file, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }
        return new PageCacheImpl((int) memory / PAGE_SIZE, raf, fc);
    }

}
