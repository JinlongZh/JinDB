package com.jinlong.jindb.backend.dm;

import com.google.common.primitives.Bytes;
import com.jinlong.jindb.backend.common.SubArray;
import com.jinlong.jindb.backend.dm.dataItem.DataItem;
import com.jinlong.jindb.backend.dm.logger.Logger;
import com.jinlong.jindb.backend.dm.page.Page;
import com.jinlong.jindb.backend.dm.page.PageX;
import com.jinlong.jindb.backend.dm.pageCache.PageCache;
import com.jinlong.jindb.backend.tm.TransactionManager;
import com.jinlong.jindb.backend.utils.Panic;
import com.jinlong.jindb.backend.utils.Parser;

import java.util.*;

/**
 * 对数据库进行恢复
 *
 * @Author zjl
 * @Date 2024/5/5
 */
public class Recover {

    private static final byte LOG_TYPE_INSERT = 0;
    private static final byte LOG_TYPE_UPDATE = 1;

    private static final int REDO = 0;
    private static final int UNDO = 1;

    static class InsertLogInfo {
        long xid;
        int pageNo;
        short offset;
        byte[] raw;
    }

    static class UpdateLogInfo {
        long xid;
        int pageNo;
        short offset;
        byte[] oldRaw;
        byte[] newRaw;
    }

    public static void recover(TransactionManager transactionManager, Logger logger, PageCache pageCache) {
        System.out.println("Recovering...");

        logger.rewind();
        // 1. 找出之前最大的页号, 并将DB文件扩充到该页号的大小的空间.
        int maxPageNo = 0;
        while (true) {
            byte[] log = logger.next();
            if (log == null) break;
            int pageNo;
            if (isInsertLog(log)) {
                InsertLogInfo insertLogInfo = parseInsertLog(log);
                pageNo = insertLogInfo.pageNo;
            } else {
                UpdateLogInfo updateLogInfo = parseUpdateLog(log);
                pageNo = updateLogInfo.pageNo;
            }
            if (pageNo > maxPageNo) {
                maxPageNo = pageNo;
            }
        }
        // 即使maxPageNo为0, page1是能被DM保证在磁盘上的
        if (maxPageNo == 0) {
            maxPageNo = 1;
        }
        pageCache.truncateByPageNo(maxPageNo);
        System.out.println("Truncate to " + maxPageNo + " pages.");

        // 2. redo所有非active的事务.
        redoTransactions(transactionManager, logger, pageCache);
        System.out.println("Redo Transactions Over.");

        // 3. undo所有active的事务.
        undoTransactions(transactionManager, logger, pageCache);
        System.out.println("Undo Transactions Over.");

        System.out.println("Recovery Over.");
    }

    private static boolean isInsertLog(byte[] log) {
        return log[0] == LOG_TYPE_INSERT;
    }

    private static void redoTransactions(TransactionManager transactionManager, Logger logger, PageCache pageCache) {
        logger.rewind();
        while (true) {
            byte[] log = logger.next();
            if (log == null) break;
            if (isInsertLog(log)) {
                InsertLogInfo insertLogInfo = parseInsertLog(log);
                long xid = insertLogInfo.xid;
                if (!transactionManager.isActive(xid)) {
                    doInsertLog(pageCache, log, REDO);
                }
            } else {
                UpdateLogInfo updateLogInfo = parseUpdateLog(log);
                long xid = updateLogInfo.xid;
                if (!transactionManager.isActive(xid)) {
                    doUpdateLog(pageCache, log, REDO);
                }
            }
        }
    }

    private static void undoTransactions(TransactionManager transactionManager, Logger logger, PageCache pageCache) {
        Map<Long, List<byte[]>> logCache = new HashMap<>();
        logger.rewind();
        while (true) {
            byte[] log = logger.next();
            if (log == null) break;
            if (isInsertLog(log)) {
                InsertLogInfo insertLogInfo = parseInsertLog(log);
                long xid = insertLogInfo.xid;
                if (transactionManager.isActive(xid)) {
                    if (!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            } else {
                UpdateLogInfo updateLogInfo = parseUpdateLog(log);
                long xid = updateLogInfo.xid;
                if (transactionManager.isActive(xid)) {
                    if (!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            }
        }

        // 对所有active log进行倒序undo
        for (Map.Entry<Long, List<byte[]>> entry : logCache.entrySet()) {
            List<byte[]> logs = entry.getValue();
            for (int i = logs.size() - 1; i >= 0; i--) {
                byte[] log = logs.get(i);
                if (isInsertLog(log)) {
                    doInsertLog(pageCache, log, UNDO);
                } else {
                    doUpdateLog(pageCache, log, UNDO);
                }
            }
            transactionManager.abort(entry.getKey());
        }
    }

    // [LogType] [XID] [UID] [OldRaw] [NewRaw]
    private static final int OF_TYPE = 0;
    private static final int OF_XID = OF_TYPE + 1;
    private static final int OF_UPDATE_UID = OF_XID + 8;
    private static final int OF_UPDATE_RAW = OF_UPDATE_UID + 8;

    public static byte[] updateLog(long xid, DataItem dataItem) {
        byte[] logType = {LOG_TYPE_UPDATE};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] uidRaw = Parser.long2Byte(dataItem.getUid());
        byte[] oldRaw = dataItem.getOldRaw();
        SubArray raw = dataItem.getRaw();
        byte[] newRaw = Arrays.copyOfRange(raw.raw, raw.start, raw.end);
        return Bytes.concat(logType, xidRaw, uidRaw, oldRaw, newRaw);
    }

    private static UpdateLogInfo parseUpdateLog(byte[] log) {
        UpdateLogInfo updateLogInfo = new UpdateLogInfo();
        updateLogInfo.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_UPDATE_UID));
        long uid = Parser.parseLong(Arrays.copyOfRange(log, OF_UPDATE_UID, OF_UPDATE_RAW));
        updateLogInfo.offset = (short) (uid & ((1L << 16) - 1));
        uid >>>= 32;
        updateLogInfo.pageNo = (int) (uid & ((1L << 32) - 1));
        int length = (log.length - OF_UPDATE_RAW) / 2;
        updateLogInfo.oldRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW, OF_UPDATE_RAW + length);
        updateLogInfo.newRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW + length, OF_UPDATE_RAW + length * 2);
        return updateLogInfo;
    }

    private static void doUpdateLog(PageCache pageCache, byte[] log, int flag) {
        int pageNo;
        short offset;
        byte[] raw;
        if (flag == REDO) {
            UpdateLogInfo updateLogInfo = parseUpdateLog(log);
            pageNo = updateLogInfo.pageNo;
            offset = updateLogInfo.offset;
            raw = updateLogInfo.newRaw;
        } else {
            UpdateLogInfo updateLogInfo = parseUpdateLog(log);
            pageNo = updateLogInfo.pageNo;
            offset = updateLogInfo.offset;
            raw = updateLogInfo.oldRaw;
        }
        Page page = null;
        try {
            page = pageCache.getPage(pageNo);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            PageX.recoverUpdate(page, raw, offset);
        } finally {
            page.release();
        }
    }

    // [LogType] [XID] [PageNo] [Offset] [Raw]
    private static final int OF_INSERT_PAGENO = OF_XID + 8;
    private static final int OF_INSERT_OFFSET = OF_INSERT_PAGENO + 4;
    private static final int OF_INSERT_RAW = OF_INSERT_OFFSET + 2;

    public static byte[] insertLog(long xid, Page page, byte[] raw) {
        byte[] logTypeRaw = {LOG_TYPE_INSERT};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] pageNoRaw = Parser.int2Byte(page.getPageNumber());
        byte[] offsetRaw = Parser.short2Byte(PageX.getFSO(page));
        return Bytes.concat(logTypeRaw, xidRaw, pageNoRaw, offsetRaw, raw);
    }

    private static InsertLogInfo parseInsertLog(byte[] log) {
        InsertLogInfo insertLogInfo = new InsertLogInfo();
        insertLogInfo.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_INSERT_PAGENO));
        insertLogInfo.pageNo = Parser.parseInt(Arrays.copyOfRange(log, OF_INSERT_PAGENO, OF_INSERT_OFFSET));
        insertLogInfo.offset = Parser.parseShort(Arrays.copyOfRange(log, OF_INSERT_OFFSET, OF_INSERT_RAW));
        insertLogInfo.raw = Arrays.copyOfRange(log, OF_INSERT_RAW, log.length);
        return insertLogInfo;
    }

    private static void doInsertLog(PageCache pageCache, byte[] log, int flag) {
        InsertLogInfo li = parseInsertLog(log);
        Page page = null;
        try {
            page = pageCache.getPage(li.pageNo);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            if (flag == UNDO) {
                DataItem.setDataItemRawInvalid(li.raw);
            }
            PageX.recoverInsert(page, li.raw, li.offset);
        } finally {
            page.release();
        }
    }

}
