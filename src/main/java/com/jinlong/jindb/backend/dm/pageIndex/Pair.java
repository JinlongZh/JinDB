package com.jinlong.jindb.backend.dm.pageIndex;

/**
 * Pair
 *
 * @Author zjl
 * @Date 2024/5/2
 */
public class Pair {

    public int pageNo;
    public int freeSpace;

    public Pair(int pageNo, int freeSpace) {
        this.pageNo = pageNo;
        this.freeSpace = freeSpace;
    }
}
