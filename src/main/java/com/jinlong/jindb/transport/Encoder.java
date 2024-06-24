package com.jinlong.jindb.transport;

import com.google.common.primitives.Bytes;
import com.jinlong.jindb.common.ErrorConstants;

import java.util.Arrays;

/**
 * 负责了包到二进制数据之间的转换
 * 包的二进制格式编码如下:
 * [flag] 1byte
 * [data] *
 * <p>
 * 如果flag为0, 则表示要发送的是数据. 那么data既为这份数据本身.
 * 如果flag为1, 则表示要发送的是错误. 那么data为[]byte(err.Errors()).
 *
 * @Author zjl
 * @Date 2024/6/24
 */
public class Encoder {

    public byte[] encode(Package pkg) {
        if (pkg.getErr() != null) {
            Exception err = pkg.getErr();
            String msg = "Intern server error!";
            if (err.getMessage() != null) {
                msg = err.getMessage();
            }
            return Bytes.concat(new byte[]{1}, msg.getBytes());
        } else {
            return Bytes.concat(new byte[]{0}, pkg.getData());
        }
    }

    public Package decode(byte[] data) throws Exception {
        if (data.length < 1) {
            throw ErrorConstants.InvalidPkgDataException;
        }
        if (data[0] == 0) {
            return new Package(Arrays.copyOfRange(data, 1, data.length), null);
        } else if (data[0] == 1) {
            return new Package(null, new RuntimeException(new String(Arrays.copyOfRange(data, 1, data.length))));
        } else {
            throw ErrorConstants.InvalidPkgDataException;
        }
    }

}
