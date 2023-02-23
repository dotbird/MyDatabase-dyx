package top.guoziyang.mydb.backend.tm;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.backend.utils.Panic;
import top.guoziyang.mydb.backend.utils.Parser;
import top.guoziyang.mydb.common.Error;

/**
 * 事务管理模块（TM）
 * XID文件格式：头部用8字节存此XID文件管理的事务总数，后面用1字节存每个事务的状态
 * ｜Header｜ status ｜ status｜ ... ｜status｜
 *  [8Byte]  [1Byte]  [1Byte] [...]  [1Byte]
 *
 * 构造函数需要检查XID文件是否合法，原理是用头部存的事务数量去计算最后一个事务在XID文件中的与起始位置的相对位置，
 *  再去对比XID文件的长度
 * 有个超级事务权限SUPER_XID，用于内部控制所有事务的操作。
 * 新建一个新事务的时候使用 ReentrantLock 保证线程安全性
 */
public class TransactionManagerImpl implements TransactionManager {

    // 使用8字节记录此XID文件管理的事务总数
    static final int LEN_XID_HEADER_LENGTH = 8;
    // 使用1字节记录每个事务的状态
    private static final int XID_FIELD_SIZE = 1;

    // 事务的三种状态
    private static final byte FIELD_TRAN_ACTIVE   = 0;  //active，正在进行，尚未结束
	private static final byte FIELD_TRAN_COMMITTED = 1; //committed，已提交
	private static final byte FIELD_TRAN_ABORTED  = 2;  //aborted，已撤销（回滚）

    // 超级事务，永远为commited状态
    //当一些操作想在没有申请事务的情况下进行，那么可以将操作的 XID 设置为 0。XID 为 0 的事务的状态永远是 committed。
    public static final long SUPER_XID = 0;


    static final String XID_SUFFIX = ".xid"; // XID 文件后缀
    private RandomAccessFile file;           // RandomAccessFile: 该类的实例支持 对文件的随机访问读写
    private FileChannel fc;                  // FileChannel: 用于读取、写入、映射和操作文件的通道
    private long xidCounter;
    private Lock counterLock;                // 锁

    // 生成随机访问读写的文件渠道
    TransactionManagerImpl(RandomAccessFile raf, FileChannel fc) {
        this.file = raf;
        this.fc = fc;
        counterLock = new ReentrantLock();
        checkXIDCounter(); // 检查XID文件是否合法
    }

    /**
     * 检查XID文件是否合法
     * 读取XID_FILE_HEADER中的xidcounter，根据它计算文件的理论长度，对比实际长度
     */
    private void checkXIDCounter() {
        long fileLen = 0;
        try {
            fileLen = file.length();
        } catch (IOException e1) {
            Panic.panic(Error.BadXIDFileException);
        }
        // 文件长度小于8字节的文件头，校验不通过
        if(fileLen < LEN_XID_HEADER_LENGTH) {
            // 对于校验没有通过的，会直接通过 panic 方法，强制停机。
            // 在一些基础模块中出现错误都会如此处理，无法恢复的错误只能直接停机。
            Panic.panic(Error.BadXIDFileException);
        }
        // 读取xid文件中事务的个数。allocate(文件头长度8): 分配一个新的字节缓冲区
        ByteBuffer buf = ByteBuffer.allocate(LEN_XID_HEADER_LENGTH);
        try {
            fc.position(0);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.xidCounter = Parser.parseLong(buf.array());
        // 取得最后一个事务在文件中的相对位置，也就是反推xid文件的长度
        long end = getXidPosition(this.xidCounter + 1);
        if(end != fileLen) {
            Panic.panic(Error.BadXIDFileException);
        }
    }

    // 根据事务xid取得其在xid文件中对应的位置
    private long getXidPosition(long xid) {
        return LEN_XID_HEADER_LENGTH + (xid-1)*XID_FIELD_SIZE;
    }

    /**
     * 更新事务的状态
     * @param xid 事务ID
     * @param status 事务需要改变为的状态
     */
    private void updateXID(long xid, byte status) {
        long offset = getXidPosition(xid);     // 得到其在xid文件中对应的位置
        byte[] tmp = new byte[XID_FIELD_SIZE];
        tmp[0] = status;
        ByteBuffer buf = ByteBuffer.wrap(tmp); // wrap(): 将字节数组包装到缓冲区中
        try {
            fc.position(offset); // position(): 设置此通道的文件位置
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        // 所有的文件操作在执行后都需要立刻刷入文件中，防止在崩溃后文件丢失数据
        try {
            // force(false): （强制刷盘）将数据刷出到磁盘；参数 metaData 指定是否将元数据也刷盘
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    // 将XID加一，并更新XID Header
    private void incrXIDCounter() {
        xidCounter ++;
        // wrap(): 将字节数组包装到缓冲区中
        ByteBuffer buf = ByteBuffer.wrap(Parser.long2Byte(xidCounter));
        try {
            // position(): 设置此通道的文件位置
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        try {
            // force(false): 强制同步缓存内容到文件中
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    /**
     * 开始一个事务，线程安全，并返回XID
     *
     * 1.首先设置 xidCounter+1；
     * 2.事务的状态为 active；
     * 3.随后 xidCounter 自增，并更新文件头
     */
    public long begin() {
        counterLock.lock();
        try {
            long xid = xidCounter + 1;
            updateXID(xid, FIELD_TRAN_ACTIVE);
            incrXIDCounter();
            return xid;
        } finally {
            counterLock.unlock();
        }
    }

    // 提交XID事务，更新XID文件中对应事务的状态即可
    public void commit(long xid) {
        updateXID(xid, FIELD_TRAN_COMMITTED);
    }

    // 回滚XID事务，更新XID文件中对应事务的状态即可
    public void abort(long xid) {
        updateXID(xid, FIELD_TRAN_ABORTED);
    }

    // 检测XID事务是否处于status状态
    private boolean checkXID(long xid, byte status) {
        long offset = getXidPosition(xid); // 得到其在xid文件中对应的位置
        ByteBuffer buf = ByteBuffer.wrap(new byte[XID_FIELD_SIZE]); // wrap(): 将字节数组包装到缓冲区中
        try {
            fc.position(offset); // position(): 设置此通道的文件位置
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return buf.array()[0] == status;
    }

    public boolean isActive(long xid) {
        if(xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ACTIVE);
    }

    public boolean isCommitted(long xid) {
        if(xid == SUPER_XID) return true;
        return checkXID(xid, FIELD_TRAN_COMMITTED);
    }

    public boolean isAborted(long xid) {
        if(xid == SUPER_XID) return false;
        return checkXID(xid, FIELD_TRAN_ABORTED);
    }

    // 关闭事务管理器
    public void close() {
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

}
