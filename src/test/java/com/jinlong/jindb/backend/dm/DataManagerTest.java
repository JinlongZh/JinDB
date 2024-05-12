package com.jinlong.jindb.backend.dm;

import com.jinlong.jindb.backend.common.SubArray;
import com.jinlong.jindb.backend.dm.dataItem.DataItem;
import com.jinlong.jindb.backend.dm.logger.LoggerImpl;
import com.jinlong.jindb.backend.dm.pageCache.PageCache;
import com.jinlong.jindb.backend.dm.pageCache.PageCacheImpl;
import com.jinlong.jindb.backend.tm.MockTransactionManager;
import com.jinlong.jindb.backend.tm.TransactionManager;
import com.jinlong.jindb.backend.tm.TransactionManagerImpl;
import com.jinlong.jindb.backend.utils.Panic;
import com.jinlong.jindb.backend.utils.RandomUtil;
import org.junit.Test;

import java.io.File;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * DataManagerTest
 *
 * @Author zjl
 * @Date 2024/5/4
 */
public class DataManagerTest {

    static List<Long> uids0, uids1;
    static Lock uidsLock;

    static Random random = new SecureRandom();

    private static final String FILE_PATH = "D:\\桌面\\TestDMSingle";

    private void initUids() {
        uids0 = new ArrayList<>();
        uids1 = new ArrayList<>();
        uidsLock = new ReentrantLock();
    }

    @Test
    public void test() throws Exception {
        TransactionManager transactionManager0 = new MockTransactionManager();
        DataManager dataManager0 = DataManager.create(FILE_PATH, PageCache.PAGE_SIZE * 10, transactionManager0);
        DataManager mockDataManager = MockDataManager.newMockDataManager();
        try {
            for (int i = 0; i < 1; i++) {
                int dataLen = 3;
                byte[] data = RandomUtil.randomBytes(dataLen);
                long uid1 = dataManager0.insert(0, data);
                long uid2 = mockDataManager.insert(0, data);

                byte[] data2 = RandomUtil.randomBytes(dataLen);
                long uid3 = dataManager0.insert(0, data2);
                long uid4 = mockDataManager.insert(0, data2);

                DataItem dataItem1 = dataManager0.read(uid1);
                DataItem dataItem2 = mockDataManager.read(uid2);
                DataItem dataItem3 = dataManager0.read(uid3);
                DataItem dataItem4 = mockDataManager.read(uid4);

                SubArray s0 = dataItem1.data();
                SubArray s1 = dataItem2.data();
                SubArray s2 = dataItem3.data();
                SubArray s3 = dataItem4.data();
                System.out.println(Arrays.toString(Arrays.copyOfRange(s0.raw, s0.start, s0.end)));
                System.out.println(Arrays.toString(Arrays.copyOfRange(s1.raw, s1.start, s1.end)));
                System.out.println(Arrays.toString(Arrays.copyOfRange(s2.raw, s2.start, s2.end)));
                System.out.println(Arrays.toString(Arrays.copyOfRange(s3.raw, s3.start, s3.end)));

            }
        } finally {
            dataManager0.close();
            mockDataManager.close();
            boolean delete1 = new File(FILE_PATH + PageCacheImpl.DB_SUFFIX).delete();
            boolean delete2 = new File(FILE_PATH + LoggerImpl.LOG_SUFFIX).delete();
            System.out.println("delete result: " + (delete1 && delete2));
        }
    }

    @Test
    public void testDMSingle() throws Exception {
        TransactionManager transactionManager0 = new MockTransactionManager();
        DataManager dataManager0 = DataManager.create(FILE_PATH, PageCache.PAGE_SIZE * 10, transactionManager0);
        DataManager mockDataManager = MockDataManager.newMockDataManager();
        try {
            int tasksNum = 10;
            CountDownLatch countDownLatch = new CountDownLatch(1);
            initUids();
            Runnable r = () -> worker(dataManager0, mockDataManager, tasksNum, 50, countDownLatch);
            new Thread(r).start();
            countDownLatch.await();
        } finally {
            dataManager0.close();
            mockDataManager.close();
            boolean delete1 = new File(FILE_PATH + PageCacheImpl.DB_SUFFIX).delete();
            boolean delete2 = new File(FILE_PATH + LoggerImpl.LOG_SUFFIX).delete();
            System.out.println("delete result: " + (delete1 && delete2));
        }
    }


    private static final String FILE_PATH1 = "D:\\桌面\\TestDMMulti";

    @Test
    public void testDMMulti() throws InterruptedException {
        int ThreadNum = 10;
        TransactionManager transactionManager = new MockTransactionManager();
        DataManager dataManager = DataManager.create(FILE_PATH1, PageCache.PAGE_SIZE * 10, transactionManager);
        DataManager mockDataManager = MockDataManager.newMockDataManager();
        CountDownLatch countDownLatch = new CountDownLatch(ThreadNum);

        try {
            int tasksNum = 500;

            initUids();
            for (int i = 0; i < ThreadNum; i++) {
                Runnable r = () -> worker(dataManager, mockDataManager, tasksNum, 50, countDownLatch);
                new Thread(r).run();
            }
            countDownLatch.await();
        } finally {
            dataManager.close();
            mockDataManager.close();
            boolean delete1 = new File(FILE_PATH1 + PageCacheImpl.DB_SUFFIX).delete();
            boolean delete2 = new File(FILE_PATH1 + LoggerImpl.LOG_SUFFIX).delete();
            System.out.println("delete result: " + (delete1 && delete2));
        }


    }

    private void worker(DataManager dm0, DataManager dm1, int tasksNum, int insertRation, CountDownLatch countDownLatch) {
        int dataLen = 60;
        try {
            for (int i = 0; i < tasksNum; i++) {
                int op = Math.abs(random.nextInt()) % 100;
                if (op < insertRation) {
                    byte[] data = RandomUtil.randomBytes(dataLen);
                    long u0, u1 = 0;
                    try {
                        u0 = dm0.insert(0, data);
                    } catch (Exception e) {
                        continue;
                    }
                    try {
                        u1 = dm1.insert(0, data);
                    } catch (Exception e) {
                        Panic.panic(e);
                    }
                    uidsLock.lock();
                    uids0.add(u0);
                    uids1.add(u1);
                    uidsLock.unlock();
                } else {
                    uidsLock.lock();
                    if (uids0.size() == 0) {
                        uidsLock.unlock();
                        continue;
                    }
                    int tmp = Math.abs(random.nextInt()) % uids0.size();
                    long u0 = uids0.get(tmp);
                    long u1 = uids1.get(tmp);
                    DataItem data0 = null, data1 = null;
                    try {
                        data0 = dm0.read(u0);
                    } catch (Exception e) {
                        Panic.panic(e);
                        continue;
                    }
                    if (data0 == null) continue;
                    try {
                        data1 = dm1.read(u1);
                    } catch (Exception e) {
                        Panic.panic(e);
                        continue;
                    }

                    data0.rLock();
                    data1.rLock();
                    SubArray s0 = data0.data();
                    SubArray s1 = data1.data();
                    assert Arrays.equals(
                            Arrays.copyOfRange(s0.raw, s0.start, s0.end),
                            Arrays.copyOfRange(s1.raw, s1.start, s1.end)
                    );

                    data0.rUnLock();
                    data1.rUnLock();

                    // 测试更新
                    byte[] newData = RandomUtil.randomBytes(dataLen);
                    data0.before();
                    data1.before();
                    System.arraycopy(newData, 0, s0.raw, s0.start, dataLen);
                    System.arraycopy(newData, 0, s1.raw, s1.start, dataLen);
                    data0.after(0);
                    data1.after(0);
                    data0.release();
                    data1.release();
                }
            }
        } finally {
            countDownLatch.countDown();
        }
    }


    private static final String FILE_PATH2 = "D:\\桌面\\TestRecoverySimple";

    /**
     * 测试恢复
     *
     * @param
     * @Return void
     */
    @Test
    public void testRecoverySimple() throws InterruptedException {
        TransactionManager tm0 = TransactionManager.create(FILE_PATH2);
        DataManager dm0 = DataManager.create(FILE_PATH2, PageCache.PAGE_SIZE * 30, tm0);
        DataManager mdm = MockDataManager.newMockDataManager();
        dm0.close();

        try {
            initUids();
            int workerNums = 10;
            for (int i = 0; i < 8; i++) {
                dm0 = DataManager.open(FILE_PATH2, PageCache.PAGE_SIZE * 10, tm0);
                CountDownLatch cdl = new CountDownLatch(workerNums);
                for (int k = 0; k < workerNums; k++) {
                    final DataManager dm = dm0;
                    Runnable r = () -> worker(dm, mdm, 100, 50, cdl);
                    new Thread(r).run();
                }
                cdl.await();
            }
        } finally {
            tm0.close();
            dm0.close();
            mdm.close();

            boolean delete = new File(FILE_PATH2 + PageCacheImpl.DB_SUFFIX).delete();
            boolean delete1 = new File(FILE_PATH2 + LoggerImpl.LOG_SUFFIX).delete();
            boolean delete2 = new File(FILE_PATH2 + TransactionManagerImpl.XID_SUFFIX).delete();
            System.out.println("delete:" + delete + "," + delete1 + "," + delete2);
        }


    }

}
