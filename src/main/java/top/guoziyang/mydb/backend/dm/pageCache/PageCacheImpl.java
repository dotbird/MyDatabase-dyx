package top.guoziyang.mydb.backend.dm.pageCache;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.backend.common.AbstractCache;
import top.guoziyang.mydb.backend.dm.page.Page;
import top.guoziyang.mydb.backend.dm.page.PageImpl;
import top.guoziyang.mydb.backend.utils.Panic;
import top.guoziyang.mydb.common.Error;

/**
 * 页面缓存的具体实现类
 * 继承抽象缓存框架，并且实现 getForCache() 和 releaseForCache() 两个抽象方法
 */
public class PageCacheImpl extends AbstractCache<Page> implements PageCache {
    
    private static final int MEM_MIN_LIM = 10;   //最小缓存资源数
    public static final String DB_SUFFIX = ".db";

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock fileLock;

    private AtomicInteger pageNumbers;  // 整型原子类

    PageCacheImpl(RandomAccessFile file, FileChannel fileChannel, int maxResource) {
        super(maxResource);
        if(maxResource < MEM_MIN_LIM) {   //缓存资源数太小，报错
            Panic.panic(Error.MemTooSmallException);
        }
        long length = 0;
        try {
            length = file.length(); //文件的长度，以字节为单位
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.file = file;
        this.fc = fileChannel;
        this.fileLock = new ReentrantLock();  // 可重入锁
        //使用了一个 AtomicInteger，来记录了当前打开的数据库文件有多少页。{字节数 / 8K = 页数}
        //这个数字在数据库文件被打开时就会被计算，并在新建页面时自增
        this.pageNumbers = new AtomicInteger((int)length / PAGE_SIZE);
    }

    /**
     * 新建页面
     * 同一条数据是不允许跨页存储的，这一点会从后面的章节中体现。这意味着，单条数据的大小不能超过数据库页面的大小
     */
    public int newPage(byte[] initData) {
        int pgno = pageNumbers.incrementAndGet();
        Page pg = new PageImpl(pgno, initData, null);
        flush(pg);    // 新建的页面需要立刻刷盘
        return pgno;
    }

    public Page getPage(int pgno) throws Exception {
        return get((long)pgno); // 先从缓存，再从文件
    }

    /**
     * 根据 pageNumber（long key）从 数据库文件 中读取 页数据，并包裹成 Page
     * 由于数据源就是文件系统，getForCache() 直接从文件中读取，并包裹成 Page 即可
     *      1. 求出当前页在文件中的位置
     *      2. 使用 ByteBuffer 作为字节缓冲区
     *      3. FileChannel 为文件流通道，并读取缓冲区的字节
     *      4. 操作期间加 ReentrantLock，可重入锁
     *      5. 最后包裹成 Page 页面，返回
     */
    @Override
    protected Page getForCache(long key) throws Exception {
        // 1. 求出当前页在文件中的位置
        int pgno = (int)key;      // pageNumber
        long offset = PageCacheImpl.pageOffset(pgno); //（偏移量）当前页在文件的起始位置

        // 2. 使用 ByteBuffer 作为字节缓冲区（nio：非阻塞io）
        // allocate(): '分配'一个新的字节缓冲区大小为（8K）
        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        fileLock.lock();          // 加锁

        // 3. FileChannel 为文件流通道，并读取缓冲区的字节
        try {
            fc.position(offset);  // FileChannel：文件流通道；position(): 到当前位置
            fc.read(buf);         // 读取字节缓冲区（8K）
        } catch(IOException e) {
            Panic.panic(e);       // 异常就强制停机
        }
        fileLock.unlock();        // 解锁

        // 5. 最后包裹成 Page 页面，返回
        return new PageImpl(pgno, buf.array(), this); // 包裹为page页面返回
    }

    /**
     * 驱逐页面（页面置换），只需要根据页面是否是脏页面，来决定是否需要写回文件系统
     */
    @Override
    protected void releaseForCache(Page pg) {
        if(pg.isDirty()) {      // 页面是否是脏页面
            flush(pg);          // 将当前页刷新到磁盘文件
            pg.setDirty(false); // 更改当前页脏页状态：不是脏页
        }
    }

    public void release(Page page) {
        release((long)page.getPageNumber()); // 强行释放一个缓存
    }

    public void flushPage(Page pg) {
        flush(pg);
    }

    /**
     * 刷新当前页到磁盘文件
     */
    private void flush(Page pg) {
        int pgno = pg.getPageNumber();  // 得到当前的PageNumber
        long offset = pageOffset(pgno); // 找到当前页面在文件中的起始位置

        fileLock.lock();     // 加锁
        try {
            ByteBuffer buf = ByteBuffer.wrap(pg.getData()); // wrap(): 将字节数组包装到缓冲区中
            fc.position(offset);            // 文件流通道找到位置
            fc.write(buf);                  // 写入缓冲字节
            fc.force(false);       // 将数据刷出到磁盘，但不包括元数据
        } catch(IOException e) {
            Panic.panic(e);
        } finally {
            fileLock.unlock(); // 解锁
        }
    }

    // 截断
    public void truncateByBgno(int maxPgno) {
        long size = pageOffset(maxPgno + 1);
        try {
            file.setLength(size);
        } catch (IOException e) {
            Panic.panic(e);
        }
        pageNumbers.set(maxPgno);
    }

    @Override
    public void close() {
        super.close();
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    public int getPageNumber() {
        return pageNumbers.intValue();
    }

    // 找到当前页面在文件中的起始位置
    private static long pageOffset(int pgno) {
        // 页号从 1 开始
        return (pgno-1) * PAGE_SIZE;
    }
    
}
