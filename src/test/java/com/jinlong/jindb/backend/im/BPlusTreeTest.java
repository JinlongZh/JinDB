package com.jinlong.jindb.backend.im;

import com.jinlong.jindb.backend.dm.DataManager;
import com.jinlong.jindb.backend.dm.pageCache.PageCache;
import com.jinlong.jindb.backend.tm.MockTransactionManager;
import com.jinlong.jindb.backend.tm.TransactionManager;
import org.junit.Test;

import java.io.File;
import java.util.List;

/**
 * BPlusTreeTest
 *
 * @Author zjl
 * @Date 2024/6/14
 */
public class BPlusTreeTest {

    private static final String FILE_PATH = "D:\\桌面\\TestTreeSingle";

    @Test
    public void testTreeSingle() throws Exception {
        TransactionManager tm = new MockTransactionManager();
        DataManager dm = DataManager.create(FILE_PATH, PageCache.PAGE_SIZE * 10, tm);

        try {
            long root = BPlusTree.create(dm);
            BPlusTree tree = BPlusTree.load(root, dm);

            int lim = 10000;
            for (int i = lim - 1; i >= 0; i--) {
                tree.insert(i, i);
            }

            for (int i = 0; i < lim; i++) {
                List<Long> uids = tree.search(i);
                assert uids.size() == 1;
                assert uids.get(0) == i;
            }
        } finally {
            tm.close();
            dm.close();
            assert new File(FILE_PATH + ".db").delete();
            assert new File(FILE_PATH + ".log").delete();
        }


    }

}
