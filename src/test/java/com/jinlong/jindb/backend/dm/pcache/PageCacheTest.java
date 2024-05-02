package com.jinlong.jindb.backend.dm.pcache;

import com.jinlong.jindb.backend.dm.page.Page;
import com.jinlong.jindb.backend.utils.Panic;
import com.jinlong.jindb.backend.utils.RandomUtil;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * TestPageCache
 *
 * @Author zjl
 * @Date 2024/5/1
 */
public class PageCacheTest {

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

    private PageCache pageCache1;
    private CountDownLatch countDownLatch1;
    private AtomicInteger noPages1;
    private static final int THREAD_COUNT_1 = 200;

    @Test
    public void testPageCacheMultiSimple() throws Exception {
        pageCache1 = PageCacheImpl.create(FILE_PATH, PageCache.PAGE_SIZE * 50);
        countDownLatch1 = new CountDownLatch(THREAD_COUNT_1);
        noPages1 = new AtomicInteger(0);
        for (int i = 0; i < THREAD_COUNT_1; i++) {
            int id = i;
            Runnable r = () -> worker1(id);
            new Thread(r).start();
        }
        countDownLatch1.await();

        pageCache1.close();
        assert new File(FILE_PATH).delete();
    }

    private void worker1(int id) {
        for (int i = 0; i < 80; i++) {
            int op = new Random(System.nanoTime()).nextInt(Integer.MAX_VALUE) % 20;
            if (op == 0) {
                // 多线程下测试newPage后getPage
                byte[] data = RandomUtil.randomBytes(PageCache.PAGE_SIZE);
                int pageNo = pageCache1.newPage(data);
                Page page = null;
                try {
                    page = pageCache1.getPage(pageNo);
                } catch (Exception e) {
                    Panic.panic(e);
                }
                noPages1.incrementAndGet();
                assert page != null;
                page.release();
            } else if (op < 20) {
                // 多线程下测试随机获取一页的数据
                int mod = noPages1.intValue();
                if (mod == 0) {
                    continue;
                }
                int pageNo = Math.abs(new Random(System.currentTimeMillis()).nextInt()) % mod + 1;
                Page page = null;
                try {
                    page = pageCache1.getPage(pageNo);
                } catch (Exception e) {
                    Panic.panic(e);
                }
                page.release();
            }
        }
        countDownLatch1.countDown();
    }


    private PageCache pageCache2, mockPageCache;
    private CountDownLatch countDownLatch2;
    private AtomicInteger noPages2;
    private Lock lockNew;
    private static final int THREAD_COUNT_2 = 20;

    @Test
    public void testPageCacheMulti() throws InterruptedException {
        pageCache2 = PageCacheImpl.create(FILE_PATH, PageCache.PAGE_SIZE * 10);
        mockPageCache = new MockPageCache();
        lockNew = new ReentrantLock();

        countDownLatch2 = new CountDownLatch(THREAD_COUNT_2);
        noPages2 = new AtomicInteger(0);

        for (int i = 0; i < THREAD_COUNT_2; i++) {
            int id = i;
            Runnable r = () -> worker2(id);
            new Thread(r).start();
        }
        countDownLatch2.await();

        pageCache2.close();
        mockPageCache.close();
        assert new File(FILE_PATH).delete();
    }

    private void worker2(int id) {
        for (int i = 0; i < 1000; i++) {
            int op = new Random(System.nanoTime()).nextInt(Integer.MAX_VALUE) % 20;
            if (op == 0) {
                // new page
                byte[] data = RandomUtil.randomBytes(PageCache.PAGE_SIZE);
                lockNew.lock();
                int pageNo = pageCache2.newPage(data);
                int mockPageNo = mockPageCache.newPage(data);
                assert pageNo == mockPageNo;
                lockNew.unlock();
                noPages2.incrementAndGet();
            } else if (op < 10) {
                // check
                int mod = noPages2.intValue();
                if (mod == 0) continue;
                int pageNo = Math.abs(new Random(System.currentTimeMillis()).nextInt()) % mod + 1;
                Page page = null, mockPage = null;
                try {
                    page = pageCache2.getPage(pageNo);
                } catch (Exception e) {
                    Panic.panic(e);
                }
                try {
                    mockPage = mockPageCache.getPage(pageNo);
                } catch (Exception e) {
                    Panic.panic(e);
                }
                // 此处加锁是因为获取的page是从缓存中拿到的引用，
                // 如果不加锁，则可能会有多个线程同时修改这个page，导致数据不一致
                page.lock();
                assert Arrays.equals(mockPage.getData(), page.getData());
                page.unlock();
                page.release();
            } else {
                // update
                int mod = noPages2.intValue();
                if (mod == 0) continue;
                int pageNo = Math.abs(new Random(System.currentTimeMillis()).nextInt()) % mod + 1;
                Page page = null, mockPageNo = null;
                try {
                    page = pageCache2.getPage(pageNo);
                } catch (Exception e) {
                    Panic.panic(e);
                }
                try {
                    mockPageNo = mockPageCache.getPage(pageNo);
                } catch (Exception e) {
                    Panic.panic(e);
                }
                byte[] newData = RandomUtil.randomBytes(PageCache.PAGE_SIZE);

                page.lock();
                mockPageNo.setDirty(true);
                for (int j = 0; j < PageCache.PAGE_SIZE; j++) {
                    mockPageNo.getData()[j] = newData[j];
                }
                page.setDirty(true);
                for (int j = 0; j < PageCache.PAGE_SIZE; j++) {
                    page.getData()[j] = newData[j];
                }
                page.unlock();
                page.release();
            }
        }
        countDownLatch2.countDown();
    }

}
