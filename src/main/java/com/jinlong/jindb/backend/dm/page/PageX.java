package com.jinlong.jindb.backend.dm.page;

import com.jinlong.jindb.backend.dm.pageCache.PageCache;
import com.jinlong.jindb.backend.utils.Parser;

import java.util.Arrays;

/**
 * PageX管理普通页
 * 普通页结构
 * [FreeSpaceOffset] [Data]
 * FreeSpaceOffset: 2字节 空闲位置开始偏移
 *
 * @Author zjl
 * @Date 2024/5/2
 */
public class PageX {

    private static final short OF_FREE = 0;
    private static final short OF_DATA = 2;
    public static final int MAX_FREE_SPACE = PageCache.PAGE_SIZE - OF_DATA;

    public static byte[] initRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setFSO(raw, OF_DATA);
        return raw;
    }

    private static void setFSO(byte[] raw, short ofData) {
        System.arraycopy(Parser.short2Byte(ofData), 0, raw, OF_FREE, OF_DATA);
    }

    /**
     * 获取page的FSO
     *
     * @param page 页面
     * @Return short
     */
    public static short getFSO(Page page) {
        return getFSO(page.getData());
    }

    private static short getFSO(byte[] raw) {
        return Parser.parseShort(Arrays.copyOfRange(raw, 0, 2));
    }

    /**
     * 将raw插入page中，返回插入位置
     *
     * @param page 页面
     * @param raw  插入的值
     * @Return short
     */
    public static short insert(Page page, byte[] raw) {
        page.setDirty(true);
        short offset = getFSO(page.getData());
        System.arraycopy(raw, 0, page.getData(), offset, raw.length);
        setFSO(page.getData(), (short) (offset + raw.length));
        return offset;
    }

    /**
     * 获取页面的空闲空间大小
     *
     * @param page 页面
     * @Return int
     */
    public static int getFreeSpace(Page page) {
        return PageCache.PAGE_SIZE - (int) getFSO(page.getData());
    }

    /**
     * 将raw插入page中的offset位置，并将page的offset设置为较大的offset
     *
     * @param page   页面
     * @param raw    插入的值
     * @param offset offset
     * @Return void
     */
    public static void recoverInsert(Page page, byte[] raw, short offset) {
        page.setDirty(true);
        System.arraycopy(raw, 0, page.getData(), offset, raw.length);

        short rawFSO = getFSO(page.getData());
        if (rawFSO < offset + raw.length) {
            setFSO(page.getData(), (short) (offset + raw.length));
        }
    }

    /**
     * 将raw插入page中的offset位置，不更新update
     *
     * @param page   页面
     * @param raw    插入的值
     * @param offset offset
     * @Return void
     */
    public static void recoverUpdate(Page page, byte[] raw, short offset) {
        page.setDirty(true);
        System.arraycopy(raw, 0, page.getData(), offset, raw.length);
    }

}
