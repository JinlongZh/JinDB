package com.jinlong.jindb.backend.dm;

import com.jinlong.jindb.backend.dm.dataItem.DataItem;

/**
 * DataManagerImpl
 *
 * @Author zjl
 * @Date 2024/5/2
 */
public class DataManagerImpl implements DataManager {

    @Override
    public DataItem read(long uid) throws Exception {
        return null;
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        return 0;
    }

    @Override
    public void close() {

    }

    public void logDataItem(long xid, DataItem dataItem) {

    }

    public void releaseDataItem(DataItem dataItem) {

    }

}
