package com.jinlong.jindb.backend.dm;

import com.jinlong.jindb.backend.dm.dataItem.DataItem;
import com.jinlong.jindb.backend.dm.logger.Logger;
import com.jinlong.jindb.backend.dm.page.PageFirst;
import com.jinlong.jindb.backend.dm.pageCache.PageCache;
import com.jinlong.jindb.backend.tm.TransactionManager;

/**
 * DataManager
 *
 * @Author zjl
 * @Date 2024/5/2
 */
public interface DataManager {

    DataItem read(long uid) throws Exception;

    long insert(long xid, byte[] data) throws Exception;

    void close();

    static DataManager create(String path, long memory, TransactionManager transactionManager) {
        PageCache pageCache = PageCache.create(path, memory);
        Logger logger = Logger.create(path);

        DataManagerImpl dataManager = new DataManagerImpl(pageCache, logger, transactionManager);
        dataManager.initPageFirst();
        return dataManager;
    }

    static DataManager open(String path, long memory, TransactionManager transactionManager) {
        PageCache pageCache = PageCache.open(path, memory);
        Logger logger = Logger.open(path);

        DataManagerImpl dataManager = new DataManagerImpl(pageCache, logger, transactionManager);
        if (!dataManager.loadCheckPageFirst()) {
            Recover.recover(transactionManager, logger, pageCache);
        }
        dataManager.fillPageIndex();
        PageFirst.setVcOpen(dataManager.pageFirst);
        dataManager.pageCache.flushPage(dataManager.pageFirst);

        return dataManager;
    }

}
