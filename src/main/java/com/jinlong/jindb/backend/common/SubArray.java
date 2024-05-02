package com.jinlong.jindb.backend.common;

/**
 * 代替共享内存数组
 *
 * @Author zjl
 * @Date 2024/5/2
 */
public class SubArray {

    public byte[] raw;
    public int start;
    public int end;

    public SubArray(byte[] raw, int start, int end) {
        this.raw = raw;
        this.start = start;
        this.end = end;
    }

}
