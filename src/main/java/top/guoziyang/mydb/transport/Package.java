package top.guoziyang.mydb.transport;

/**
 * C/S 客户端和服务端传输的基本结构
 * 将sql语句和错误一起打包
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
