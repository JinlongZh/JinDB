package com.jinlong.jindb.backend.tm;

import org.junit.Test;

import java.io.File;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 事务管理器测试
 *
 * @Author zjl
 * @Date 2024/4/27
 */
public class TransactionManagerTest {

    private TransactionManager transactionManager;
    private Map<Long, Byte> transactionMap;
    private int TransactionCount = 0;
    private static final int threadCount = 100;
    private static final int testCount = 100;
    private CountDownLatch countDownLatch;
    private final Lock lock = new ReentrantLock();

    private static final String FILE_PATH = "D:\\桌面\\transactionManager_test.xid";

    @Test
    public void testByMultiThread() {
        transactionManager = TransactionManager.create(FILE_PATH);
        transactionMap = new ConcurrentHashMap<>();
        countDownLatch = new CountDownLatch(threadCount);
        for (int i = 0; i < threadCount; i++) {
            Runnable runner = this::worker;
            new Thread(runner, "Thread" + i).start();
        }
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        transactionManager.close();
        assert new File(FILE_PATH).delete();
    }

    private void worker() {
        boolean inTransaction = false;
        long transactionXID = 0;
        for (int i = 0; i < testCount; i++) {
            // 操作事务
            lock.lock();
            if (!inTransaction) {
                long xid = transactionManager.begin();
                transactionMap.put(xid, (byte) 0);
                TransactionCount++;
                transactionXID = xid;
                inTransaction = true;
//                System.out.println(Thread.currentThread().getName() + " 开启事务，XID: " + xid);
            } else {
                int status = (new Random(System.nanoTime()).nextInt(Integer.MAX_VALUE) % 2) + 1;
                switch (status) {
                    case 1:
                        transactionManager.commit(transactionXID);
//                        System.out.println(Thread.currentThread().getName() + " 提交事务，XID: " + transactionXID);
                        break;
                    case 2:
                        transactionManager.abort(transactionXID);
//                        System.out.println(Thread.currentThread().getName() + " 回滚事务，XID: " + transactionXID);
                        break;
                }
                transactionMap.put(transactionXID, (byte) status);
                inTransaction = false;
            }
            lock.unlock();

            // 验证事务
            lock.lock();
            Set<Map.Entry<Long, Byte>> entries = transactionMap.entrySet();
            for (Map.Entry<Long, Byte> entry : entries) {
                boolean ok = false;
                Long xid = entry.getKey();
                Byte status = entry.getValue();
                switch (status) {
                    case 0:
                        ok = transactionManager.isActive(xid);
                        break;
                    case 1:
                        ok = transactionManager.isCommitted(xid);
                        break;
                    case 2:
                        ok = transactionManager.isAborted(xid);
                        break;
                }
                assert ok;
//                System.out.println(Thread.currentThread().getName() + " 验证事务，XID: " + xid + ", 状态: " + status + ", 结果: " + ok);
            }
            lock.unlock();

        }
        countDownLatch.countDown();
    }

}
