package com.jinlong.jindb.backend.dm.dataItem;

import com.google.common.primitives.Bytes;
import com.jinlong.jindb.backend.common.SubArray;
import com.jinlong.jindb.backend.dm.DataManagerImpl;
import com.jinlong.jindb.backend.dm.page.Page;
import com.jinlong.jindb.backend.utils.Parser;
import com.jinlong.jindb.backend.utils.Types;

import java.util.Arrays;

/**
 * DataItem
 *
 * @Author zjl
 * @Date 2024/5/2
 */
public interface DataItem {
    SubArray data();

    void before();

    void unBefore();

    void after(long xid);

    void release();

    void lock();

    void unlock();

    void rLock();

    void rUnLock();

    static byte[] wrapDataItemRaw(byte[] raw) {
        byte[] valid = new byte[1];
        byte[] size = Parser.short2Byte((short) raw.length);
        return Bytes.concat(valid, size, raw);
    }

    /**
     * 从页面的offset处解析处dataItem
     *
     * @param page        页面
     * @param offset      offset
     * @param dataManager dm
     * @Return DataItem
     */
    static DataItem parseDataItem(Page page, short offset, DataManagerImpl dataManager) {
        byte[] raw = page.getData();
        short size = Parser.parseShort(Arrays.copyOfRange(raw, offset + DataItemImpl.OF_SIZE, offset + DataItemImpl.OF_DATA));
        short length = (short) (size + DataItemImpl.OF_DATA);
        long uid = Types.addressToUid(page.getPageNumber(), offset);
        return new DataItemImpl(new SubArray(raw, offset, offset + length), new byte[length], page, uid, dataManager);
    }

    static void setDataItemRawInvalid(byte[] raw) {
        raw[DataItemImpl.OF_VALID] = (byte) 1;
    }
}
