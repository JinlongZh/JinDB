package com.jinlong.jindb.client;

import com.jinlong.jindb.transport.Encoder;
import com.jinlong.jindb.transport.Packager;
import com.jinlong.jindb.transport.Transporter;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

/**
 * 客户端启动器
 *
 * @Author zjl
 * @Date 2024/6/27
 */
public class Launcher {

    public static void main(String[] args) throws UnknownHostException, IOException {
        Socket socket = new Socket("127.0.0.1", 9999);
        Encoder encoder = new Encoder();
        Transporter transporter = new Transporter(socket);
        Packager packager = new Packager(transporter, encoder);

        Client client = new Client(packager);
        Shell shell = new Shell(client);
        shell.run();
    }

}
