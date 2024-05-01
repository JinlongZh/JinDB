package com.jinlong.jindb.backend.dm.page;

/**
 * 页面接口
 *
 * @Author zjl
 * @Date 2024/5/1
 */
public interface Page {

    void lock();
    void unlock();
    void release();
    void setDirty(boolean dirty);
    boolean isDirty();
    int getPageNumber();
    byte[] getData();

}
