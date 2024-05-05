package com.jinlong.jindb.backend.dm;

import com.jinlong.jindb.backend.common.AbstractRefCountCache;
import com.jinlong.jindb.backend.dm.dataItem.DataItem;
import com.jinlong.jindb.backend.dm.dataItem.DataItemImpl;
import com.jinlong.jindb.backend.dm.logger.Logger;
import com.jinlong.jindb.backend.dm.page.Page;
import com.jinlong.jindb.backend.dm.page.PageFirst;
import com.jinlong.jindb.backend.dm.page.PageX;
import com.jinlong.jindb.backend.dm.pageIndex.PageIndex;
import com.jinlong.jindb.backend.dm.pageIndex.Pair;
import com.jinlong.jindb.backend.dm.pageCache.PageCache;
import com.jinlong.jindb.backend.tm.TransactionManager;
import com.jinlong.jindb.backend.utils.Panic;
import com.jinlong.jindb.backend.utils.Types;

/**
 * DataManagerImpl
 *
 * @Author zjl
 * @Date 2024/5/2
 */
public class DataManagerImpl extends AbstractRefCountCache<DataItem> implements DataManager {

    TransactionManager transactionManager;
    PageCache pageCache;
    Logger logger;
    PageIndex pageIndex;
    Page pageFirst;

    public DataManagerImpl(PageCache pageCache, Logger logger, TransactionManager transactionManager) {
        // 实际的内存限制实际上是在pageCache中, 所以这里应该设置为0, 表示无限
        super(0);
        this.pageCache = pageCache;
        this.logger = logger;
        this.transactionManager = transactionManager;
        this.pageIndex = new PageIndex();
    }


    @Override
    public DataItem read(long uid) throws Exception {
        DataItemImpl dataItem = (DataItemImpl) super.get(uid);
        if (!dataItem.isValid()) {
            dataItem.release();
            return null;
        }
        return dataItem;
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        byte[] raw = DataItem.wrapDataItemRaw(data);
        if (raw.length > PageX.MAX_FREE_SPACE) {
            throw new RuntimeException("Data too large");
        }

        Pair pair = null;
        for (int i = 0; i < 5; i++) {
            pair = pageIndex.select(raw.length);
            if (pair != null) {
                break;
            } else {
                int newPageNo = pageCache.newPage(PageX.initRaw());
                pageIndex.add(newPageNo, PageX.MAX_FREE_SPACE);
            }
        }

        if (pair == null) {
            throw new RuntimeException("Database is busy");
        }

        Page page = null;
        int freeSpace = 0;
        try {

            page = pageCache.getPage(pair.pageNo);

            byte[] log = Recover.insertLog(xid, page, raw);
            logger.log(log);

            short offset = PageX.insert(page, raw);

            page.release();
            return Types.addressToUid(pair.pageNo, offset);

        } finally {
            // 将取出的page重新插入pageIndex
            if (page != null) {
                pageIndex.add(pair.pageNo, PageX.getFreeSpace(page));
            } else {
                pageIndex.add(pair.pageNo, freeSpace);
            }
        }
    }

    @Override
    public void close() {
        super.close();
        logger.close();

        // 关于pageFirst的操作一定要在Close中被最后执行.
        PageFirst.setVcClose(pageFirst);
        pageFirst.release();
        pageCache.close();
    }

    @Override
    protected DataItem getForCache(long uid) throws Exception {
        short offset = (short) (uid & ((1L << 16) - 1));
        uid >>>= 32;
        int pageNo = (int) (uid & ((1L << 32) - 1));
        Page page = pageCache.getPage(pageNo);
        return DataItem.parseDataItem(page, offset, this);
    }

    @Override
    protected void releaseForCache(DataItem dataItem) {
        dataItem.getPage().release();
    }

    /**
     * 为xid生成update日志
     *
     * @param xid
     * @param dataItem
     * @Return void
     */
    public void logDataItem(long xid, DataItem dataItem) {
        byte[] log = Recover.updateLog(xid, dataItem);
        logger.log(log);
    }

    public void releaseDataItem(DataItem dataItem) {
        super.release(dataItem.getUid());
    }

    /**
     * 在创建文件时初始化PageOne
     *
     * @param
     * @Return void
     */
    void initPageFirst() {
        int pageNo = pageCache.newPage(PageFirst.InitRaw());
        assert pageNo == 1;
        try {
            pageFirst = pageCache.getPage(pageNo);
        } catch (Exception e) {
            Panic.panic(e);
        }
        pageCache.flushPage(pageFirst);
    }

    /**
     * 在打开已有文件时时读入PageOne，并验证正确性
     *
     * @param
     * @Return boolean
     */
    boolean loadCheckPageFirst() {
        try {
            pageFirst = pageCache.getPage(1);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return PageFirst.checkVc(pageFirst);
    }

    /**
     * 初始化pageIndex
     *
     * @param
     * @Return void
     */
    void fillPageIndex() {
        int pageNumber = pageCache.getPageNumber();
        for (int i = 2; i <= pageNumber; i++) {
            Page pg = null;
            try {
                pg = pageCache.getPage(i);
            } catch (Exception e) {
                Panic.panic(e);
            }
            pageIndex.add(pg.getPageNumber(), PageX.getFreeSpace(pg));
            pg.release();
        }
    }

}
