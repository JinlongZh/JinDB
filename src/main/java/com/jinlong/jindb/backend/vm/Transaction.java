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
    // 隔离度
    public int level;
    // 快照
    public Map<Long, Boolean> snapshot;
    // 发生的错误， 该事务只能被回滚
    public Exception err;
    // 该事务是否被自动回滚
    public boolean autoAborted;

    public static Transaction newTransaction(long xid, int level, Map<Long, Transaction> active) {
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
        // 忽略SUPER_XID
        if (xid == TransactionManagerImpl.SUPER_XID) {
            return false;
        }
        return snapshot.containsKey(xid);
    }

}
