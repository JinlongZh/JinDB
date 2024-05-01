package com.jinlong.jindb.backend.common;

import com.jinlong.jindb.backend.utils.Panic;
import com.jinlong.jindb.common.ErrorConstants;
import org.junit.Test;

import java.security.SecureRandom;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * CacheTest
 *
 * @Author zjl
 * @Date 2024/5/1
 */
public class CacheTest {
    static Random random = new SecureRandom();

    private CountDownLatch countDownLatch;
    private MockLRUCache mockLRUCache;
    private MockRefCountCache mockRefCountCache;

    private static final Integer THREAD_NUM = 200;

    @Test
    public void testLRUCache() {
        mockLRUCache = new MockLRUCache();
        countDownLatch = new CountDownLatch(THREAD_NUM);
        for(int i = 0; i < THREAD_NUM; i ++) {
            Runnable r = () -> workLRU();
            new Thread(r).start();
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    @Test
    public void testRefCountCache() {
        mockRefCountCache = new MockRefCountCache();
        countDownLatch = new CountDownLatch(THREAD_NUM);
        for(int i = 0; i < THREAD_NUM; i ++) {
            Runnable r = () -> workRefCount();
            new Thread(r).start();
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private void workLRU() {
        for(int i = 0; i < 1000; i++) {
//            long uid = random.nextInt();
            long uid = new Random(System.nanoTime()).nextInt();
            long h = 0;
            try {
                h = mockLRUCache.get(uid);
            } catch (Exception e) {
                Panic.panic(e);
            }
            assert h == uid;
        }
        countDownLatch.countDown();
    }

    private void workRefCount() {
        for(int i = 0; i < 1000; i++) {
//            long uid = random.nextInt();
            long uid = new Random(System.nanoTime()).nextInt();
            long h = 0;
            try {
                h = mockRefCountCache.get(uid);
            } catch (Exception e) {
                if(e == ErrorConstants.CacheFullException) continue;
                Panic.panic(e);
            }
            assert h == uid;
        }
        countDownLatch.countDown();
    }

}
