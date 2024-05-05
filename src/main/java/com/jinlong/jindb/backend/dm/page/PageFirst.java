package com.jinlong.jindb.backend.dm.page;

import com.jinlong.jindb.backend.dm.pageCache.PageCache;
import com.jinlong.jindb.backend.utils.RandomUtil;

import java.util.Arrays;

/**
 * 特殊管理第一页
 * ValidCheck
 * db启动时给100 ~ 107 字节处填入一个随机字节，db关闭时将其拷贝到108 ~ 115 字节
 * 在每次重启时，都检验两个区间内的值是否一致，如果不一致，则说明上次为正常结束，则对数据库进行恢复.
 *
 * @Author zjl
 * @Date 2024/5/2
 */
public class PageFirst {

    private static final int OF_VC = 100;
    private static final int LEN_VC = 8;

    public static byte[] InitRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setVcOpen(raw);
        return raw;
    }

    public static void setVcOpen(Page page) {
        page.setDirty(true);
        setVcOpen(page.getData());
    }

    private static void setVcOpen(byte[] raw) {
        System.arraycopy(RandomUtil.randomBytes(LEN_VC), 0, raw, OF_VC, LEN_VC);
    }

    public static void setVcClose(Page page) {
        page.setDirty(true);
        setVcClose(page.getData());
    }

    private static void setVcClose(byte[] raw) {
        System.arraycopy(raw, OF_VC, raw, OF_VC + LEN_VC, LEN_VC);
    }

    public static boolean checkVc(Page pg) {
        return checkVc(pg.getData());
    }

    private static boolean checkVc(byte[] raw) {
        return Arrays.equals(
                Arrays.copyOfRange(raw, OF_VC, OF_VC + LEN_VC),
                Arrays.copyOfRange(raw, OF_VC + LEN_VC, OF_VC + 2 * LEN_VC)
        );
    }

}
