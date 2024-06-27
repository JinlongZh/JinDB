package com.jinlong.jindb.client;

import com.jinlong.jindb.transport.Package;
import com.jinlong.jindb.transport.Packager;

/**
 * client实现了客户端的API和命令行UI
 *
 *    大概结构为:
 *         [shell]       [user process]
 *             |              |
 *             v              |
 *         [client] <---------+
 *             |
 *             v
 *       [RoundTripper]
 *
 *     shell为用户提供了一个简单的命令行形式的UI.
 *     也可以不用shell, 自己编写程序, 然后调用client作为访问数据库的API.
 *     client将需要数据库执行的指令打包, 并传递给RoundTripper.
 *     RoundTripper进行一次包的"发送->接受"工作.
 *     RoundTripper依赖于transporter包.
 *
 * @Author zjl
 * @Date 2024/6/27
 */
public class Client {

    private RoundTripper roundTripper;

    public Client(Packager packager) {
        this.roundTripper = new RoundTripper(packager);
    }

    public byte[] execute(byte[] stat) throws Exception {
        Package pkg = new Package(stat, null);
        Package resPackage = roundTripper.roundTrip(pkg);
        if (resPackage.getErr() != null) {
            throw resPackage.getErr();
        }
        return resPackage.getData();
    }

    public void close() {
        try {
            roundTripper.close();
        } catch (Exception e) {
        }
    }

}
