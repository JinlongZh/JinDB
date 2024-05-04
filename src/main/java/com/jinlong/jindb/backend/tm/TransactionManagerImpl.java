package com.jinlong.jindb.backend.tm;

import com.jinlong.jindb.backend.utils.Panic;
import com.jinlong.jindb.backend.utils.Parser;
import com.jinlong.jindb.common.ErrorConstants;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 事务管理器
 *
 * @Author zjl
 * @Date 2024/4/27
 */
public class TransactionManagerImpl implements TransactionManager {

    /*
       0. active       未结束
       1. committed    已经被提交
       2. aborted      已经被撤销
     */
    private static final byte FIELD_TRAN_ACTIVE = 0;
    private static final byte FIELD_TRAN_COMMITTED = 1;
    private static final byte FIELD_TRAN_ABORTED = 2;

    // super事务，为0
    public static final long SUPER_XID = 0;

    // XID文件头长度
    static final int LEN_XID_HEADER_LENGTH = 8;
    // xid文件中为每个事务指定了1byte的空间用于存储其状态。
    private static final int XID_FIELD_SIZE = 1;

    private RandomAccessFile file;
    private FileChannel fc;
    private long xidCounter;
    private Lock counterLock;

    public TransactionManagerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        counterLock = new ReentrantLock();
        checkXIDCounter();
    }

    /**
     * 检查XID文件是否合法
     * 读取XID_FILE_HEADER中的xidCounter，根据它计算文件的理论长度，对比实际长度
     *
     * @Return void
     */
    private void checkXIDCounter() {
        try {
            long fileLen = file.length();
            if (fileLen < LEN_XID_HEADER_LENGTH) {
                Panic.panic(ErrorConstants.BadXIDFileException);
            }

            ByteBuffer buf = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);
            fc.position(0);
            fc.read(buf);
            this.xidCounter = Parser.parseLong(buf.array());

            // 检查文件长度时候和XID匹配
            long end = getXidPosition(this.xidCounter + 1);
            if (end != fileLen) {
                Panic.panic(ErrorConstants.BadXIDFileException);
            }
        } catch (IOException e) {
            Panic.panic(e);
        }
    }


    /**
     * 开始一个事务，并返回XID
     *
     * @Return long XID
     */
    public long begin() {
        counterLock.lock();
        try {
            long xid = xidCounter + 1;
            updateXID(xid, FIELD_TRAN_ACTIVE);
            incrXIDCounter();
            return xid;
        } finally {
            counterLock.unlock();
        }
    }

    /**
     * 提交XID事务
     *
     * @param xid XID
     * @Return void
     */
    public void commit(long xid) {
        updateXID(xid, FIELD_TRAN_COMMITTED);
    }

    /**
     * 回滚XID事务
     *
     * @param xid XID
     * @Return void
     */
    public void abort(long xid) {
        updateXID(xid, FIELD_TRAN_ABORTED);
    }

    /**
     * 根据事务xid取得其在xid文件中对应的位置
     *
     * @param xid xid
     * @Return long xid文件中对应的位置
     */
    private long getXidPosition(long xid) {
        return LEN_XID_HEADER_LENGTH + (xid - 1) * XID_FIELD_SIZE;
    }

    /**
     * 更新xid事务的状态为status
     *
     * @param xid    XID
     * @param status 需要更新的状态
     * @Return void
     */
    private void updateXID(long xid, byte status) {
        long offset = getXidPosition(xid);
        byte[] tmp = new byte[XID_FIELD_SIZE];
        tmp[0] = status;
        ByteBuffer buf = ByteBuffer.wrap(tmp);
        try {
            fc.position(offset);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            fc.force(true);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 将XID加一，并更新XID Header
     *
     * @Return void
     */
    private void incrXIDCounter() {
        xidCounter++;
        ByteBuffer buf = ByteBuffer.wrap(Parser.long2Byte(xidCounter));
        try {
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            fc.force(true);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 检测XID事务是否处于status状态
     *
     * @param xid    xid
     * @param status 需要确定的目标状态
     * @Return boolean
     */
    private boolean checkXID(long xid, byte status) {
        long offset = getXidPosition(xid);
        ByteBuffer buf = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]);
        try {
            fc.position(offset);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return buf.array()[0] == status;
    }

    public boolean isActive(long xid) {
        if (xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ACTIVE);
    }

    public boolean isCommitted(long xid) {
        if (xid == SUPER_XID) return true;
        return checkXID(xid, FIELD_TRAN_COMMITTED);
    }

    public boolean isAborted(long xid) {
        if (xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ABORTED);
    }

    public void close() {
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }


}
