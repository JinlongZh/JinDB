package com.jinlong.jindb.backend.dm.pcache;

import com.jinlong.jindb.backend.dm.page.Page;

/**
 * 页面缓存接口
 *
 * @Author zjl
 * @Date 2024/5/1
 */
public interface PageCache {

    // 页面大小：8k
    public static final int PAGE_SIZE = 1 << 13;

    int newPage(byte[] initData);
    Page getPage(int pageNo) throws Exception;
    void close();
    void release(Page page);

    void truncateByPageNo(int maxPageNo);
    int getPageNumber();
    void flushPage(Page page);

}
