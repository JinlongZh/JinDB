package com.jinlong.jindb.backend.utils;

import com.google.common.primitives.Bytes;

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
     * 将 short 类型的值转换为字节数组
     *
     * @param value short 类型的值
     * @return 字节数组
     */
    public static byte[] short2Byte(short value) {
        return ByteBuffer.allocate(Short.SIZE / Byte.SIZE).putShort(value).array();
    }

    /**
     * 将字节数组解析为 short 类型的值
     *
     * @param buf 字节数组
     * @return short 类型的值
     */
    public static short parseShort(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 2);
        return buffer.getShort();
    }

    /**
     * 将 int 类型的值转换为字节数组
     *
     * @param value int 类型的值
     * @return 字节数组
     */
    public static byte[] int2Byte(int value) {
        return ByteBuffer.allocate(Integer.SIZE / Byte.SIZE).putInt(value).array();
    }

    /**
     * 将字节数组解析为 int 类型的值
     *
     * @param buf 字节数组
     * @return int 类型的值
     */
    public static int parseInt(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 4);
        return buffer.getInt();
    }

    /**
     * 将字节数组解析为 long 类型的值
     *
     * @param buf 字节数组
     * @return long 类型的值
     */
    public static long parseLong(byte[] buf) {
        ByteBuffer buffer = ByteBuffer.wrap(buf, 0, 8);
        return buffer.getLong();
    }

    /**
     * 将 long 类型的值转换为字节数组
     *
     * @param value long 类型的值
     * @return 字节数组
     */
    public static byte[] long2Byte(long value) {
        return ByteBuffer.allocate(Long.SIZE / Byte.SIZE).putLong(value).array();
    }

    public static ParseStringRes parseString(byte[] raw) {
        int length = parseInt(Arrays.copyOf(raw, 4));
        String str = new String(Arrays.copyOfRange(raw, 4, 4 + length));
        return new ParseStringRes(str, length + 4);
    }

    public static byte[] string2Byte(String str) {
        byte[] l = int2Byte(str.length());
        return Bytes.concat(l, str.getBytes());
    }

    public static long str2Uid(String key) {
        long seed = 13331;
        long res = 0;
        for (byte b : key.getBytes()) {
            res = res * seed + (long) b;
        }
        return res;
    }

}

