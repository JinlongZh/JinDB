package com.jinlong.jindb.transport;

/**
 * 包裹了protocoler和transporter, 为用户提供了包的收发接口.
 *
 * @Author zjl
 * @Date 2024/6/24
 */
public class Packager {

    private Transporter transpoter;
    private Encoder encoder;

    public Packager(Transporter transporter, Encoder encoder) {
        this.transpoter = transporter;
        this.encoder = encoder;
    }

    public void send(Package pkg) throws Exception {
        byte[] data = encoder.encode(pkg);
        transpoter.send(data);
    }

    public Package receive() throws Exception {
        byte[] data = transpoter.receive();
        return encoder.decode(data);
    }

    public void close() throws Exception {
        transpoter.close();
    }

}
