package com.jinlong.jindb.backend.dm.logger;

import com.jinlong.jindb.backend.utils.Panic;
import com.jinlong.jindb.backend.utils.Parser;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * 日志
 *
 * @Author zjl
 * @Date 2024/5/5
 */
public interface Logger {

    void log(byte[] data);

    void truncate(long x) throws Exception;

    byte[] next();

    void rewind();

    void close();

    static Logger create(String path) {
        String fileName = path + LoggerImpl.LOG_SUFFIX;
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

        ByteBuffer buf = ByteBuffer.wrap(Parser.int2Byte(0));
        try {
            fc.position(0);
            fc.write(buf);
            fc.force(true);
        } catch (IOException e) {
            Panic.panic(e);
        }

        return new LoggerImpl(raf, fc, 0);
    }

    static Logger open(String path) {
        String fileName = path + LoggerImpl.LOG_SUFFIX;
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

        LoggerImpl logger = new LoggerImpl(raf, fc);
        logger.init();

        return logger;
    }

}
