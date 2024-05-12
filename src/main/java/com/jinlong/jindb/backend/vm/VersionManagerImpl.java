package com.jinlong.jindb.backend.vm;

import com.jinlong.jindb.backend.dm.DataManager;

/**
 * VersionManager实现类
 *
 * @Author zjl
 * @Date 2024/5/12
 */
public class VersionManagerImpl implements VersionManager {

    DataManager dataManager;

    @Override
    public byte[] read(long xid, long uid) throws Exception {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public boolean delete(long xid, long uid) throws Exception {
        // TODO Auto-generated method stub
        return false;
    }

    @Override
    public long begin(int level) {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void commit(long xid) throws Exception {
        // TODO Auto-generated method stub

    }

    @Override
    public void abort(long xid) {
        // TODO Auto-generated method stub

    }

    public void releaseEntry(Entry entry) {
    }

}
