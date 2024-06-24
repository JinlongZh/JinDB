package com.jinlong.jindb.transport;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Hex;

import java.io.*;
import java.net.Socket;

/**
 * Transporter 负责了二进制数据的传送和接受.
 * <p>
 * Transporter有自己的二进制数据传输协议, 协议内容为:
 * 首先将二进制数据按照高4位和低4位拆分, 目的是为了干掉特殊字符, 如换行符.
 * 接着, 再拆分后的二进制数据后, 补上一个换行符\n, 并发送.
 * 那么, 另一端的Transporter就可以以readLine的形式, 读取出这一段传送的数据.
 * 接受到数据后, 去掉最后的换行符, 再将二进制数据的按照之前拆分的逆方法, 进行组装.
 * 最后得到完整的二进制数据.
 *
 * @Author zjl
 * @Date 2024/6/24
 */
public class Transporter {

    private Socket socket;
    private BufferedReader reader;
    private BufferedWriter writer;

    public Transporter(Socket socket) throws IOException {
        this.socket = socket;
        this.reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        this.writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
    }

    public void send(byte[] data) throws Exception {
        String raw = hexEncode(data);
        writer.write(raw);
        writer.flush();
    }

    public byte[] receive() throws Exception {
        String line = reader.readLine();
        if (line == null) {
            close();
        }
        return hexDecode(line);
    }

    public void close() throws IOException {
        writer.close();
        reader.close();
        socket.close();
    }

    private String hexEncode(byte[] buf) {
        return Hex.encodeHexString(buf, true) + "\n";
    }

    private byte[] hexDecode(String buf) throws DecoderException {
        return Hex.decodeHex(buf);
    }

}
