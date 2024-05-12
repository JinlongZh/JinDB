package com.jinlong.jindb.backend.vm;

import com.jinlong.jindb.backend.tm.TransactionManagerImpl;

import java.util.HashMap;
import java.util.Map;

/**
 * VM对一个事务的抽象
 *
 * @Author zjl
 * @Date 2024/5/12
 */
public class Transaction {

    public long xid;
    public int level;
    public Map<Long, Boolean> snapshot;
    public Exception err;
    public boolean autoAborted;

    public static Transaction newTransaction(long xid, int level, Map<Long, Boolean> active) {
        Transaction transaction = new Transaction();
        transaction.xid = xid;
        transaction.level = level;
        if (level != 0) {
            transaction.snapshot = new HashMap<>();
            for (Long x : active.keySet()) {
                transaction.snapshot.put(x, true);
            }
        }
        return transaction;
    }

    public boolean isInSnapshot(long xid) {
        if (xid == TransactionManagerImpl.SUPER_XID) {
            return false;
        }
        return snapshot.containsKey(xid);
    }

}
