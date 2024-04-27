package com.jinlong.jindb.backend.utils;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * Parser
 *
 * @Author zjl
 * @Date 2024/4/27
 */
public class Parser {

    /**
     * 将字节数组转换为一个64位的长整型值
     *
     * @param buf 字节数组
     * @Return long 转换后的长整型
     */
    public static long parseLong(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 8);
        return buffer.getLong();
    }

    /**
     * 将一个64位的长整型值转换为字节数组
     *
     * @param value 需要转换的长整型值
     * @Return byte 转换后的字节数组
     */
    public static byte[] long2Byte(long value) {
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(value).array();
    }

}

