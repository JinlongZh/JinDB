package com.jinlong.jindb.client;

import com.jinlong.jindb.transport.Package;
import com.jinlong.jindb.transport.Packager;

/**
 * 模拟一次收发包的过程
 *
 * @Author zjl
 * @Date 2024/6/27
 */
public class RoundTripper {

    private Packager packager;

    public RoundTripper(Packager packager) {
        this.packager = packager;
    }

    public Package roundTrip(Package pkg) throws Exception {
        packager.send(pkg);
        return packager.receive();
    }

    public void close() throws Exception {
        packager.close();
    }

}
