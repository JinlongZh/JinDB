package com.jinlong.jindb.backend.vm;

import com.jinlong.jindb.backend.dm.DataManager;
import com.jinlong.jindb.backend.tm.TransactionManager;

/**
 * VersionManager接口
 *
 * @Author zjl
 * @Date 2024/5/12
 */
public interface VersionManager {

    byte[] read(long xid, long uid) throws Exception;

    long insert(long xid, byte[] data) throws Exception;

    boolean delete(long xid, long uid) throws Exception;

    long begin(int level);

    void commit(long xid) throws Exception;

    void abort(long xid);

    static VersionManager newVersionManager(TransactionManager transactionManager, DataManager dataManager) {
        return new VersionManagerImpl(transactionManager, dataManager);
    }

}
