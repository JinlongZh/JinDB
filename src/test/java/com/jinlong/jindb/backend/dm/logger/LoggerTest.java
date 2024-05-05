package com.jinlong.jindb.backend.dm.logger;

import org.junit.Test;

import java.io.File;

/**
 * LoggerTest
 *
 * @Author zjl
 * @Date 2024/5/5
 */
public class LoggerTest {

    private static final String FILE_PATH = "D:\\桌面\\logger_test";

    @Test
    public void testLogger() {
        Logger logger = Logger.create(FILE_PATH);
        try {
            logger.log("aaa".getBytes());
            logger.log("bbb".getBytes());
            logger.log("ccc".getBytes());
            logger.log("ddd".getBytes());
            logger.log("eee".getBytes());
            logger.close();

            logger = Logger.open(FILE_PATH);
            logger.rewind();

            byte[] log = logger.next();
            assert log != null;
            assert "aaa".equals(new String(log));

            log = logger.next();
            assert log != null;
            assert "bbb".equals(new String(log));

            log = logger.next();
            assert log != null;
            assert "ccc".equals(new String(log));

            log = logger.next();
            assert log != null;
            assert "ddd".equals(new String(log));

            log = logger.next();
            assert log != null;
            assert "eee".equals(new String(log));

            log = logger.next();
            assert log == null;

        } finally {
            logger.close();
            boolean delete = new File(FILE_PATH + LoggerImpl.LOG_SUFFIX).delete();
            System.out.println("delete file: " + delete);
        }

    }

}
