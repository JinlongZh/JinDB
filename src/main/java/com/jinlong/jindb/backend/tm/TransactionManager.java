package com.jinlong.jindb.backend.tm;

import com.jinlong.jindb.backend.utils.Panic;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 描述
 *
 * @Author zjl
 * @Date 2024/5/4
 */
public interface TransactionManager {

    long begin();
    void commit(long xid);
    void abort(long xid);
    boolean isActive(long xid);
    boolean isCommitted(long xid);
    boolean isAborted(long xid);
    void close();

    /**
     * 创建文件管理器
     *
     * @param path 文件路径
     * @Return TransactionManager 返回事务管理器
     */
    static TransactionManagerImpl create(String path) {
        String fileName = path + TransactionManagerImpl.XID_SUFFIX;

        File f = new File(fileName);

        try {
            // 检查文件是否存在
            if (f.exists()) {
                Panic.panic(new IllegalStateException("File already exists: " + fileName));
            }
            // 创建文件
            if (!f.createNewFile()) {
                Panic.panic(new IllegalStateException("Failed to create f: " + fileName));
            }
            // 检查读写权限
            if (!f.canRead() || !f.canWrite()) {
                Panic.panic(new IllegalStateException("No read/write permissions on f: " + fileName));
            }
        } catch (IOException e) {
            Panic.panic(e);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        // 初始化XID文件头
        ByteBuffer buf = ByteBuffer.wrap(new byte[TransactionManagerImpl.LEN_XID_HEADER_LENGTH]);
        try {
            assert fc != null;
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        // 返回事务管理器
        return new TransactionManagerImpl(raf, fc);
    }

    /**
     * 打开文件管理器
     *
     * @param path 文件路径
     * @Return TransactionManager 返回事务管理器
     */
    static TransactionManagerImpl open(String path) {
        String fileName = path + TransactionManagerImpl.XID_SUFFIX;

        File file = new File(fileName);

        // 检查文件是否存在
        if (!file.exists()) {
            throw new IllegalStateException("File does not exist: " + fileName);
        }

        // 检查读写权限
        if (!file.canRead() || !file.canWrite()) {
            throw new IllegalStateException("No read/write permissions on file: " + fileName);
        }

        try {
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            FileChannel fc = raf.getChannel();
            // 返回事务管理器
            return new TransactionManagerImpl(raf, fc);
        } catch (FileNotFoundException e) {
            throw new IllegalStateException("Error opening file: " + fileName, e);
        }
    }

}
