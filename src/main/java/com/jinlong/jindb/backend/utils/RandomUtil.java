package com.jinlong.jindb.backend.utils;

import java.util.Random;

/**
 * 随机数工具类
 *
 * @Author zjl
 * @Date 2024/5/1
 */
public class RandomUtil {

    public static byte[] randomBytes(int length) {
        Random r = new Random(System.nanoTime());
        byte[] buf = new byte[length];
        for(int i = 0; i < length; i ++) {
            int tmp = r.nextInt(Integer.MAX_VALUE) % 62;
            if(tmp < 26) {
                buf[i] = (byte)('a' + tmp);
            } else if(tmp < 52) {
                buf[i] = (byte)('A' + tmp - 26);
            } else {
                buf[i] = (byte)('0' + tmp - 52);
            }
        }
        return buf;
    }

}
