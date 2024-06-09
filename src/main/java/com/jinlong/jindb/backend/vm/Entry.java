package com.jinlong.jindb.backend.vm;

import com.google.common.primitives.Bytes;
import com.jinlong.jindb.backend.common.SubArray;
import com.jinlong.jindb.backend.dm.dataItem.DataItem;
import com.jinlong.jindb.backend.utils.Parser;

import java.util.Arrays;

/**
 * VM向上层抽象出entry，维护了VM中记录的结构
 * entry结构：
 * [XMIN] [XMAX] [data]
 *
 * @Author zjl
 * @Date 2024/5/12
 */
public class Entry {

    private static final int OF_XMIN = 0;
    private static final int OF_XMAX = OF_XMIN + 8;
    private static final int OF_DATA = OF_XMAX + 8;

    private long uid;
    private DataItem dataItem;
    private VersionManager versionManager;

    public static Entry newEntry(VersionManager versionManager, DataItem dataItem, long uid) {
        if (dataItem == null) {
            return null;
        }
        Entry entry = new Entry();
        entry.uid = uid;
        entry.dataItem = dataItem;
        entry.versionManager = versionManager;
        return entry;
    }

    public static Entry loadEntry(VersionManager versionManager, long uid) throws Exception {
        DataItem di = ((VersionManagerImpl) versionManager).dataManager.read(uid);
        return newEntry(versionManager, di, uid);
    }

    /**
     * 将xid和data包裹成entry的二进制数据.
     */
    public static byte[] wrapEntryRaw(long xid, byte[] data) {
        byte[] xmin = Parser.long2Byte(xid);
        byte[] xmax = new byte[8];
        return Bytes.concat(xmin, xmax, data);
    }

    public void release() {
        ((VersionManagerImpl) versionManager).releaseEntry(this);
    }

    public void remove() {
        dataItem.release();
    }

    /**
     * 以拷贝的形式返回内容
     */
    public byte[] data() {
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            byte[] data = new byte[sa.end - sa.start - OF_DATA];
            System.arraycopy(sa.raw, sa.start + OF_DATA, data, 0, data.length);
            return data;
        } finally {
            dataItem.rUnLock();
        }
    }

    public long getXmin() {
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start + OF_XMIN, sa.start + OF_XMAX));
        } finally {
            dataItem.rUnLock();
        }
    }

    public long getXmax() {
        dataItem.rLock();
        try {
            SubArray sa = dataItem.data();
            return Parser.parseLong(Arrays.copyOfRange(sa.raw, sa.start + OF_XMAX, sa.start + OF_DATA));
        } finally {
            dataItem.rUnLock();
        }
    }

    public void setXmax(long xid) {
        dataItem.before();
        try {
            SubArray sa = dataItem.data();
            System.arraycopy(Parser.long2Byte(xid), 0, sa.raw, sa.start + OF_XMAX, 8);
        } finally {
            dataItem.after(xid);
        }
    }

    public long getUid() {
        return uid;
    }

}
