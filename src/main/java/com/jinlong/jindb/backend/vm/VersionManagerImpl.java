package com.jinlong.jindb.backend.vm;

import com.jinlong.jindb.backend.common.AbstractLRUCache;
import com.jinlong.jindb.backend.dm.DataManager;
import com.jinlong.jindb.backend.tm.TransactionManager;
import com.jinlong.jindb.backend.tm.TransactionManagerImpl;
import com.jinlong.jindb.backend.utils.Panic;
import com.jinlong.jindb.common.ErrorConstants;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * VersionManager实现类, 保证了调度的可串行化, 同时实现了MVCC.
 * <p>
 * 当事务发生ErrCannotSR错误时, VM会对该事务进行自动回滚.
 *
 * @Author zjl
 * @Date 2024/5/12
 */
public class VersionManagerImpl extends AbstractLRUCache<Entry> implements VersionManager {

    TransactionManager transactionManager;
    DataManager dataManager;
    Map<Long, Transaction> activeTransaction;
    Lock lock;
    LockTable lockTable;

    public VersionManagerImpl(TransactionManager transactionManager, DataManager dataManager) {
        super(0);
        this.transactionManager = transactionManager;
        this.dataManager = dataManager;
        this.activeTransaction = new HashMap<>();
        activeTransaction.put(TransactionManagerImpl.SUPER_XID, Transaction.newTransaction(TransactionManagerImpl.SUPER_XID, 0, null));
        this.lock = new ReentrantLock();
        this.lockTable = new LockTable();
    }

    @Override
    public byte[] read(long xid, long uid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if (t.err != null) {
            throw t.err;
        }

        Entry entry = null;
        try {
            entry = super.get(uid);
        } catch (Exception e) {
            if (e == ErrorConstants.NullEntryException) {
                return null;
            } else {
                throw e;
            }
        }
        try {
            if (Visibility.isVisible(transactionManager, t, entry)) {
                return entry.data();
            } else {
                return null;
            }
        } finally {
            entry.release();
        }
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        if (t.err != null) {
            throw t.err;
        }

        byte[] raw = Entry.wrapEntryRaw(xid, data);
        return dataManager.insert(xid, raw);
    }

    @Override
    public boolean delete(long xid, long uid) throws Exception {
        lock.lock();
        Transaction transaction = activeTransaction.get(xid);
        lock.unlock();

        if (transaction.err != null) {
            throw transaction.err;
        }
        Entry entry = null;
        try {
            entry = super.get(uid);
        } catch (Exception e) {
            if (e == ErrorConstants.NullEntryException) {
                return false;
            } else {
                throw e;
            }
        }
        try {
            if (!Visibility.isVisible(transactionManager, transaction, entry)) {
                return false;
            }
            // 先读取并判空, 再判断死锁
            Lock l = null;
            try {
                l = lockTable.add(xid, uid);
            } catch (Exception e) {
                transaction.err = ErrorConstants.ConcurrentUpdateException;
                internAbort(xid, true);
                transaction.autoAborted = true;
                throw transaction.err;
            }
            if (l != null) {
                l.lock();
                l.unlock();
            }

            if (entry.getXmax() == xid) {
                return false;
            }

            if (Visibility.isVersionSkip(transactionManager, transaction, entry)) {
                transaction.err = ErrorConstants.ConcurrentUpdateException;
                internAbort(xid, true);
                transaction.autoAborted = true;
                throw transaction.err;
            }

            entry.setXmax(xid);
            return true;
        } finally {
            entry.release();
        }
    }

    @Override
    public long begin(int level) {
        lock.lock();
        try {
            long xid = transactionManager.begin();
            Transaction t = Transaction.newTransaction(xid, level, activeTransaction);
            activeTransaction.put(xid, t);
            return xid;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void commit(long xid) throws Exception {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        lock.unlock();

        try {
            if (t.err != null) {
                throw t.err;
            }
        } catch (NullPointerException n) {
            System.out.println("xid: " + xid + "异常, 当前活跃事务: " + activeTransaction.keySet());
            Panic.panic(n);
        }

        lock.lock();
        activeTransaction.remove(xid);
        lock.unlock();

        lockTable.remove(xid);
        transactionManager.commit(xid);
    }

    @Override
    public void abort(long xid) {
        internAbort(xid, false);
    }

    private void internAbort(long xid, boolean autoAborted) {
        lock.lock();
        Transaction t = activeTransaction.get(xid);
        if (!autoAborted) {
            activeTransaction.remove(xid);
        }
        lock.unlock();

        if (t.autoAborted) return;
        lockTable.remove(xid);
        transactionManager.abort(xid);
    }

    public void releaseEntry(Entry entry) {
        super.release(entry.getUid());
    }

    @Override
    protected Entry getForCache(long uid) throws Exception {
        Entry entry = Entry.loadEntry(this, uid);
        if (entry == null) {
            throw ErrorConstants.NullEntryException;
        }
        return entry;
    }

    @Override
    protected void releaseForCache(Entry entry) {
        entry.remove();
    }
}
