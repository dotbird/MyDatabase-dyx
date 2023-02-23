package top.guoziyang.mydb.backend.dm.logger;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.google.common.primitives.Bytes;

import top.guoziyang.mydb.backend.utils.Panic;
import top.guoziyang.mydb.backend.utils.Parser;
import top.guoziyang.mydb.common.Error;

/**
 * 日志文件读写
 * 
 * 日志文件标准格式为：
 * [XChecksum] [Log1] [Log2] ... [LogN] [BadTail]
 *    - XChecksum 4字节，为后续所有日志计算的 Checksum，int类型
 *    - Log1 ~ LogN 是常规的日志数据
 *    - BadTail 是在数据库崩溃时，没有来得及写完的日志数据，这个 BadTail 不一定存在
 * 
 * 每条正确日志的格式为：一个[Log1]
 * [Size] [Checksum] [Data]
 *    - Size 4字节int 标识Data长度
 *    - Checksum 4字节int，是该条日志的校验和
 */
public class LoggerImpl implements Logger {

    private static final int SEED = 13331;  // 校验和使用的“种子”

    private static final int OF_SIZE = 0;
    private static final int OF_CHECKSUM = OF_SIZE + 4; // [XChecksum] = 4 字节
    private static final int OF_DATA = OF_CHECKSUM + 4; // [Size] + [Checksum] = 4 + 4 = 8 字节
    
    public static final String LOG_SUFFIX = ".log";

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock lock;

    private long position;  // 当前日志指针的位置
    private long fileSize;  // 初始化时记录，log操作不更新
    private int xChecksum;  // 一个四字节的整数，对后续所有日志计算的校验和

    LoggerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        lock = new ReentrantLock();
    }

    LoggerImpl(RandomAccessFile raf, FileChannel fc, int xChecksum) {
        this.file = raf;
        this.fc = fc;
        this.xChecksum = xChecksum;
        lock = new ReentrantLock();
    }

    // 初始化
    void init() {
        long size = 0;
        try {
            size = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        if(size < 4) {
            Panic.panic(Error.BadLogFileException);
        }

        ByteBuffer raw = ByteBuffer.allocate(4);
        try {
            fc.position(0);
            fc.read(raw);
        } catch (IOException e) {
            Panic.panic(e);
        }
        int xChecksum = Parser.parseInt(raw.array());
        this.fileSize = size;
        this.xChecksum = xChecksum;

        checkAndRemoveTail();
    }

    /** 检查 并 移除bad tail
     *
     * 在打开一个日志文件时，需要首先校验日志文件的 XChecksum，并移除文件尾部可能存在的 BadTail，
     * 由于 BadTail 该条日志尚未写入完成，文件的校验和也就不会包含该日志的校验和，
     * 去掉 BadTail 即可保证日志文件的一致性。
     */
    private void checkAndRemoveTail() {
        rewind();    // 倒带，指针指到 4

        int xCheck = 0;
        while(true) {
            byte[] log = internNext();  // 迭代 log，全部检查一遍
            if(log == null) break;      // 迭代完log，没有错误；下一句为 null,跳出循环*
            //xCheck = calChecksum(xCheck, log); // 计算单条日志的校验和（这应该 += 因为下面比较总校验和了）
            xCheck += calChecksum(xCheck, log);  // 我自己改为了 “+=”
        }
        if(xCheck != xChecksum) {
            Panic.panic(Error.BadLogFileException);
        }

        try {
            truncate(position);  // 截断文件到正常日志的末尾*
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            file.seek(position);  // 设置文件中，指针的偏移量
        } catch (IOException e) {
            Panic.panic(e);
        }
        rewind();    // 倒带，指针指到 4
    }

    // 计算单条日志的校验和
    // 通过一个指定的'种子'实现的。这样，对所有日志求出校验和，求和就能得到日志文件的校验和了
    private int calChecksum(int xCheck, byte[] log) {
        for (byte b : log) {
            xCheck = xCheck * SEED + b;
        }
        return xCheck;
    }

    // 截断文件到正常日志的末尾。truncate(): 截断
    @Override
    public void truncate(long x) throws Exception {
        lock.lock();
        try {
            // 将通道的文件截断为给定大小（若给定的大小小于文件的当前大小，那么文件将被截断，丢弃文件结尾以外的任何字节）
            fc.truncate(x);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 向日志文件写入日志
     *
     * 首先将数据包裹成日志格式，写入文件后，再更新文件的校验和，更新校验和时，会刷新缓冲区，保证内容写入磁盘。
     * @param data
     */
    @Override
    public void log(byte[] data) {
        byte[] log = wrapLog(data);     // wrapLog():将'数据'包裹成 日志格式
        ByteBuffer buf = ByteBuffer.wrap(log); // 包裹 log 为 buffer
        lock.lock();
        try {
            fc.position(fc.size());  // 定位
            fc.write(buf);           // 写入文件
        } catch(IOException e) {
            Panic.panic(e);
        } finally {
            lock.unlock();
        }
        updateXChecksum(log);        // 更新文件的校验和（包含刷盘）
    }

    // 更新文件的校验和（包含刷盘）
    private void updateXChecksum(byte[] log) {
        this.xChecksum = calChecksum(this.xChecksum, log);
        try {
            fc.position(0);  // 定位
            fc.write(ByteBuffer.wrap(Parser.int2Byte(xChecksum))); // 写入
            fc.force(false);   // 刷盘，内容写入磁盘
        } catch(IOException e) {
            Panic.panic(e);
        }
    }

    // 将数据包裹成日志格式: [Size] [Checksum] [Data]
    private byte[] wrapLog(byte[] data) {
        byte[] checksum = Parser.int2Byte(calChecksum(0, data)); // 得到[Checksum]
        byte[] size = Parser.int2Byte(data.length); // 得到[Size]
        // concat(xx,xx,xx): 合并多个数组
        return Bytes.concat(size, checksum, data);  // 合并为日志格式：[Size] [Checksum] [Data]
    }

    /**
     * 迭代器中的 next() 语句，下一句。
     *
     * Logger 被实现成迭代器模式，通过 next() 方法，不断地从文件中读取下一条日志，并将其中的 Data 解析出来并返回。
     * next() 方法的实现主要依靠 internNext()，大致如下，其中 position 是当前日志文件读到的位置偏移
     *
     * 每条正确日志的格式为：[Size] [Checksum] [Data]
     *  1. 读取 [size]
     *  2. 读取 [checksum] + [data]
     *  3. 校验 [checksum]
     *
     * @return byte[] log
     */
    private byte[] internNext() {
        if(position + OF_DATA >= fileSize) {  // 位置偏移 + (大小位+校验位) >= 文件大小
            return null;
        }

        // 1. 读取 [size]
        ByteBuffer tmp = ByteBuffer.allocate(4); // 分配一个新的buffer数组
        try {
            fc.position(position);  // 文件通道，定位
            fc.read(tmp);           // 读取到tmp
        } catch(IOException e) {
            Panic.panic(e);
        }
        int size = Parser.parseInt(tmp.array()); // size: tmp的长度 4（里面保存信息为Data的长度）
        if(position + size + OF_DATA > fileSize) {
            return null;
        }

        // 2. 读取 [checksum] + [data]
        ByteBuffer buf = ByteBuffer.allocate(OF_DATA + size);
        try {
            fc.position(position);
            fc.read(buf);   // 读取到 buf
        } catch(IOException e) {
            Panic.panic(e);
        }
        byte[] log = buf.array();

        // 3. 校验 [checksum]
        // 计算 log 的校验和（从位置8开始，到完）
        int checkSum1 = calChecksum(0, Arrays.copyOfRange(log, OF_DATA, log.length));
        // 取出 log 自己的检验和（4-8位）
        int checkSum2 = Parser.parseInt(Arrays.copyOfRange(log, OF_CHECKSUM, OF_DATA));
        if(checkSum1 != checkSum2) {  // 校验是否相等
            return null;
        }
        position += log.length; // 日志指针的位置更新（为指向下一条做准备）*
        return log;
    }

    // 加锁，调 internNext()方法
    @Override
    public byte[] next() {
        lock.lock();
        try {
            byte[] log = internNext();  // 调internNext()方法
            if(log == null) return null;
            return Arrays.copyOfRange(log, OF_DATA, log.length);
        } finally {
            lock.unlock();
        }
    }

    // rewind(): 倒带，指针指到4
    @Override
    public void rewind() {
        position = 4;
    }

    @Override
    public void close() {
        try {
            fc.close();
            file.close();
        } catch(IOException e) {
            Panic.panic(e);
        }
    }
    
}
