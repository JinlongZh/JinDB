package com.jinlong.jindb.transport;

import com.jinlong.jindb.backend.utils.Panic;
import org.junit.Test;

import java.net.ServerSocket;
import java.net.Socket;

/**
 * PackagerTest
 *
 * @Author zjl
 * @Date 2024/6/24
 */
public class PackagerTest {

    @Test
    public void testPackager() throws Exception {
        new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    ServerSocket serverSocket = new ServerSocket(10345);
                    Socket socket = serverSocket.accept();
                    Transporter transporter = new Transporter(socket);
                    Encoder encoder = new Encoder();
                    Packager packager = new Packager(transporter, encoder);
                    Package one = packager.receive();
                    assert "pkg1 test".equals(new String(one.getData()));
                    Package two = packager.receive();
                    assert "pkg2 test".equals(new String(two.getData()));
                    packager.send(new Package("pkg3 test".getBytes(), null));
                    serverSocket.close();
                } catch (Exception e) {
                    Panic.panic(e);
                }
            }
        }).start();
        Socket socket = new Socket("127.0.0.1", 10345);
        Transporter t = new Transporter(socket);
        Encoder e = new Encoder();
        Packager p = new Packager(t, e);
        p.send(new Package("pkg1 test".getBytes(), null));
        p.send(new Package("pkg2 test".getBytes(), null));
        Package three = p.receive();
        assert "pkg3 test".equals(new String(three.getData()));
    }

}
