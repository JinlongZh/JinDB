package com.jinlong.jindb.transport;

/**
 * 传输层数据包
 *
 * @Author zjl
 * @Date 2024/6/24
 */
public class Package {
    byte[] data;
    Exception err;

    public Package(byte[] data, Exception err) {
        this.data = data;
        this.err = err;
    }

    public byte[] getData() {
        return data;
    }

    public Exception getErr() {
        return err;
    }
}