package com.jinlong.jindb.backend.dm.pcache;

import com.jinlong.jindb.backend.dm.page.Page;
import org.junit.Test;

import java.io.File;

/**
 * TestPageCache
 *
 * @Author zjl
 * @Date 2024/5/1
 */
public class TestPageCache {

    private static final String FILE_PATH = "D:\\桌面\\pcacher_simple_test0.db";

    @Test
    public void testPageCache() throws Exception {
        PageCache pageCache = PageCacheImpl.create(FILE_PATH, PageCache.PAGE_SIZE * 50);
        for (int i = 0; i < 100; i++) {
            byte[] tmp = new byte[PageCache.PAGE_SIZE];
            tmp[0] = (byte) i;
            int pageNo = pageCache.newPage(tmp);
            Page page = pageCache.getPage(pageNo);
            page.setDirty(true);
            page.release();
        }
        pageCache.close();

        pageCache = PageCacheImpl.open(FILE_PATH, PageCache.PAGE_SIZE * 50);
        for (int i = 1; i <= 100; i++) {
            // 页码从1开始
            Page page = pageCache.getPage(i);
            assert page.getData()[0] == (byte) i - 1;
            page.release();
        }
        pageCache.close();

        assert new File(FILE_PATH).delete();
    }

}
