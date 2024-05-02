package com.jinlong.jindb.backend.dm.pageIndex;

import com.jinlong.jindb.backend.dm.pcache.PageCache;
import org.junit.Test;

/**
 * PageIndexTest
 *
 * @Author zjl
 * @Date 2024/5/2
 */
public class PageIndexTest {

    @Test
    public void testPageIndex() {
        PageIndex pageIndex = new PageIndex();
        int threshold = PageCache.PAGE_SIZE / 20;
        for (int i = 0; i < 20; i++) {
            pageIndex.add(i, i * threshold);
            pageIndex.add(i, i * threshold);
            pageIndex.add(i, i * threshold);
        }

        for (int k = 0; k < 3; k++) {
            for (int i = 0; i < 19; i++) {
                Pair pair = pageIndex.select(i * threshold);
                assert pair != null;
                assert pair.pageNo == i + 1;
            }
        }
    }

}
