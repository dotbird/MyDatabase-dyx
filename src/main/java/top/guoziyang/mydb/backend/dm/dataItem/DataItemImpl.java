package top.guoziyang.mydb.backend.dm.dataItem;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import top.guoziyang.mydb.backend.common.SubArray;
import top.guoziyang.mydb.backend.dm.DataManagerImpl;
import top.guoziyang.mydb.backend.dm.page.Page;

/** DataItem 的实现类
 * DataItem 是 DM 层向上层提供的数据抽象。上层模块通过地址，向 DM 请求到对应的 DataItem，再获取到其中的数据。
 *
 * dataItem 结构：[ValidFlag] [DataSize] [Data]
 *   ValidFlag 1字节，0为合法，1为非法
 *   DataSize  2字节，标识Data的长度
 */
public class DataItemImpl implements DataItem {

    static final int OF_VALID = 0;
    static final int OF_SIZE = 1;
    static final int OF_DATA = 3;

    private SubArray raw;
    private byte[] oldRaw;
    private Lock rLock;    // 读锁
    private Lock wLock;    // 写锁
    //保存一个 dm 的引用是因为其释放依赖 dm 的释放。(dm 同时实现了缓存接口，用于缓存 DataItem），以及修改数据时落日志)
    private DataManagerImpl dm;
    private long uid;
    private Page pg;

    public DataItemImpl(SubArray raw, byte[] oldRaw, Page pg, long uid, DataManagerImpl dm) {
        this.raw = raw;
        this.oldRaw = oldRaw;
        ReadWriteLock lock = new ReentrantReadWriteLock(); // 可重入读写锁*
        rLock = lock.readLock();    // 读锁
        wLock = lock.writeLock();   // 写锁
        this.dm = dm;
        this.uid = uid;
        this.pg = pg;
    }

    // 是否合法： ValidFlag 1字节，0为合法
    public boolean isValid() {
        return raw.raw[raw.start+OF_VALID] == (byte)0;
    }

    /**
     * 上层模块在获取到 DataItem 后，可以通过 data() 方法，
     * 该方法返回的数组是数据共享的，而不是拷贝实现的，所以使用了 SubArray。
     */
    @Override
    public SubArray data() {
        return new SubArray(raw.raw, raw.start+OF_DATA, raw.end); //返回的数组是数据共享的
    }

    /**
     * 在上层模块试图对 DataItem 进行修改时，需要遵循一定的流程：
     *  1. 在修改之前需要调用 before() 方法，
     *  2. 想要撤销修改时，调用 unBefore() 方法，
     *  3. 在修改完成后，调用 after() 方法。
     * 整个流程，主要是为了保存前相数据，并及时log日志。DM 会保证对 DataItem 的修改是原子性的
     */

    /** 1. 在修改之前需要调用 before() 方法
     */
    @Override
    public void before() {
        wLock.lock();        // 写锁
        pg.setDirty(true);   // 设置脏读
        System.arraycopy(raw.raw, raw.start, oldRaw, 0, oldRaw.length);// 写给老数据
    }

    /** 2. 想要撤销修改时，调用 unBefore() 方法，
     */
    @Override
    public void unBefore() {
        System.arraycopy(oldRaw, 0, raw.raw, raw.start, oldRaw.length);// 撤销修改
        wLock.unlock();     // 写锁解锁
    }

    /** 3. 在修改完成后，调用 after() 方法。写日志
     */
    @Override
    public void after(long xid) {
        dm.logDataItem(xid, this); // 为xid生成update日志
        wLock.unlock();               // 写锁解锁
    }

    /**
     * 在使用完 DataItem 后，也应当及时调用 release() 方法，释放掉 DataItem 的缓存（由 DM 缓存 DataItem）
     */
    @Override
    public void release() {
        dm.releaseDataItem(this);
    }

    @Override
    public void lock() {
        wLock.lock();
    }

    @Override
    public void unlock() {
        wLock.unlock();
    }

    @Override
    public void rLock() {
        rLock.lock();
    }

    @Override
    public void rUnLock() {
        rLock.unlock();
    }

    @Override
    public Page page() {
        return pg;
    }

    @Override
    public long getUid() {
        return uid;
    }

    @Override
    public byte[] getOldRaw() {
        return oldRaw;
    }

    @Override
    public SubArray getRaw() {
        return raw;
    }
    
}
