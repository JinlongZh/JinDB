package com.jinlong.jindb.backend.common;

import org.junit.Test;

import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * CacheTest
 *
 * @Author zjl
 * @Date 2024/5/1
 */
public class CacheTest {

    private CountDownLatch countDownLatch;
    private MockLRUCache mockLRUCache;

    @Test
    public void testLRUCache() {
        mockLRUCache = new MockLRUCache();
        countDownLatch = new CountDownLatch(1000);
        for(int i = 0; i < 1000; i ++) {
            Runnable r = () -> work();
            new Thread(r).start();
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void work() {
        for(int i = 0; i < 1000; i++) {
            long uid = new Random(System.nanoTime()).nextInt();
            long h;
            try {
                h = mockLRUCache.get(uid);
            } catch (Exception e) {
                continue;
            }
            assert h == uid;
        }
        countDownLatch.countDown();
    }

}
