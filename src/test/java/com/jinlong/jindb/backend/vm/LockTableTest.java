package com.jinlong.jindb.backend.vm;

import com.jinlong.jindb.backend.utils.Panic;
import org.junit.Test;

import java.util.concurrent.locks.Lock;

import static org.junit.Assert.assertThrows;

/**
 * LockTableTest
 *
 * @Author zjl
 * @Date 2024/6/9
 */
public class LockTableTest {

    @Test
    public void testLockTable() {
        LockTable lockTable = new LockTable();
        try {
            lockTable.add(1, 1);
        } catch (Exception e) {
            Panic.panic(e);
        }

        try {
            lockTable.add(2, 2);
        } catch (Exception e) {
            Panic.panic(e);
        }

        try {
            lockTable.add(2, 1);
        } catch (Exception e) {
            Panic.panic(e);
        }

        assertThrows(RuntimeException.class, () -> lockTable.add(1, 2));
    }

    @Test
    public void testLockTable2() {
        LockTable lockTable = new LockTable();
        for (long i = 1; i <= 100; i++) {
            try {
                Lock o = lockTable.add(i, i);
                if (o != null) {
                    Runnable r = () -> {
                        o.lock();
                        o.unlock();
                    };
                    new Thread(r).run();
                }
            } catch (Exception e) {
                Panic.panic(e);
            }
        }

        for (long i = 1; i <= 99; i++) {
            try {
                Lock o = lockTable.add(i, i + 1);
                if (o != null) {
                    Runnable r = () -> {
                        o.lock();
                        o.unlock();
                    };
                    new Thread(r).run();
                }
            } catch (Exception e) {
                Panic.panic(e);
            }
        }

        assertThrows(RuntimeException.class, () -> lockTable.add(100, 1));
        lockTable.remove(23);

        try {
            lockTable.add(100, 1);
        } catch (Exception e) {
            Panic.panic(e);
        }
    }

}
