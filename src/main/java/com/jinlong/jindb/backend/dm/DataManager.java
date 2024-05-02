package com.jinlong.jindb.backend.dm;

import com.jinlong.jindb.backend.dm.dataItem.DataItem;

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

}
