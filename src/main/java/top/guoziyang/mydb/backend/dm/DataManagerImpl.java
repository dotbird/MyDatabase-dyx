package top.guoziyang.mydb.backend.dm;

import top.guoziyang.mydb.backend.common.AbstractCache;
import top.guoziyang.mydb.backend.dm.dataItem.DataItem;
import top.guoziyang.mydb.backend.dm.dataItem.DataItemImpl;
import top.guoziyang.mydb.backend.dm.logger.Logger;
import top.guoziyang.mydb.backend.dm.page.Page;
import top.guoziyang.mydb.backend.dm.page.PageOne;
import top.guoziyang.mydb.backend.dm.page.PageX;
import top.guoziyang.mydb.backend.dm.pageCache.PageCache;
import top.guoziyang.mydb.backend.dm.pageIndex.PageIndex;
import top.guoziyang.mydb.backend.dm.pageIndex.PageInfo;
import top.guoziyang.mydb.backend.tm.TransactionManager;
import top.guoziyang.mydb.backend.utils.Panic;
import top.guoziyang.mydb.backend.utils.Types;
import top.guoziyang.mydb.common.Error;

/** DataManage 的实现类
 * DataManager 是 DM 层直接对外提供方法的类，同时，也实现成 DataItem 对象的缓存。
 * DataItem 存储的 key，是由页号和页内偏移组成的一个 8 字节无符号整数，页号和偏移各占 4 字节。
 */
public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager {

    TransactionManager tm;
    PageCache pc;
    Logger logger;
    PageIndex pIndex;
    Page pageOne;

    public DataManagerImpl(PageCache pc, Logger logger, TransactionManager tm) {
        super(0);
        this.pc = pc;
        this.logger = logger;
        this.tm = tm;
        this.pIndex = new PageIndex();
    }

    /** 读取
     * 根据 UID 从缓存中获取 DataItem，并校验有效位
     */
    @Override
    public DataItem read(long uid) throws Exception {
        DataItemImpl di = (DataItemImpl)super.get(uid); // 根据 UID 从缓存中获取 DataItem
        if(!di.isValid()) {                             // 校验有效位
            di.release();
            return null;
        }
        return di;
    }

    /** 插入
     *
     * 1.在 pageIndex 中获取一个足以存储插入内容的页面的页号，
     * 2.获取页面后，首先需要写入插入日志，
     * 3.接着才可以通过 pageX 插入数据，并返回插入位置的偏移。
     * 4.最后需要将页面信息重新插入 pageIndex
     */
    @Override
    public long insert(long xid, byte[] data) throws Exception {

        byte[] raw = DataItem.wrapDataItemRaw(data); // 打包为 DateItem
        if(raw.length > PageX.MAX_FREE_SPACE) {
            throw Error.DataTooLargeException;
        }

        // 1. 在 pageIndex 中尝试获取一个可用的页面
        PageInfo pi = null;
        for(int i = 0; i < 5; i ++) {
            pi = pIndex.select(raw.length);
            if (pi != null) {
                break;
            } else {
                int newPgno = pc.newPage(PageX.initRaw());
                pIndex.add(newPgno, PageX.MAX_FREE_SPACE);
            }
        }
        if(pi == null) {
            throw Error.DatabaseBusyException;
        }

        Page pg = null;
        int freeSpace = 0;
        try {
            pg = pc.getPage(pi.pgno);
            // 2. 先写入插入日志
            byte[] log = Recover.insertLog(xid, pg, raw);
            logger.log(log);
            // 3. 再执行插入操作
            short offset = PageX.insert(pg, raw);

            pg.release();
            return Types.addressToUid(pi.pgno, offset);

        } finally {
            // 4. 将取出的pg重新插入pIndex
            if(pg != null) {
                pIndex.add(pi.pgno, PageX.getFreeSpace(pg));
            } else {
                pIndex.add(pi.pgno, freeSpace);
            }
        }
    }

    /**
     * 正常关闭时，需要执行缓存和日志的关闭流程，不要忘了设置第一页的字节校验
     */
    @Override
    public void close() {
        super.close();
        logger.close();

        PageOne.setVcClose(pageOne); // db关闭时将100~107字节，拷贝到数组108~115字节
        pageOne.release();
        pc.close();
    }

    /**
     * 为xid生成update日志
     */
    public void logDataItem(long xid, DataItem di) {
        byte[] log = Recover.updateLog(xid, di);
        logger.log(log);
    }

    public void releaseDataItem(DataItem di) {
        super.release(di.getUid());
    }

    /** 根据 key 得到缓存
     * 只需要从 key 中解析出页号，从 pageCache 中获取到页面，再根据偏移，解析出 DataItem
     */
    @Override
    protected DataItem getForCache(long uid) throws Exception {
        short offset = (short)(uid & ((1L << 16) - 1));      // 偏移量
        uid >>>= 32;
        int pgno = (int)(uid & ((1L << 32) - 1));            // 页号
        Page pg = pc.getPage(pgno);                          // 取出缓存
        return DataItem.parseDataItem(pg, offset, this); // 从页面的offset处解析出dataitem
    }

    /**
     * 缓存释放，
     * 需要将 DataItem 写回数据源，由于对文件的读写是以页为单位进行的，
     * 只需要将 DataItem 所在的页 release 即可
     */
    @Override
    protected void releaseForCache(DataItem di) {
        di.page().release();
    }

    // 在创建文件时初始化PageOne
    void initPageOne() {
        int pgno = pc.newPage(PageOne.InitRaw());
        assert pgno == 1;
        try {
            pageOne = pc.getPage(pgno);
        } catch (Exception e) {
            Panic.panic(e);
        }
        pc.flushPage(pageOne);
    }

    // 在打开已有文件时时读入PageOne，并验证正确性
    boolean loadCheckPageOne() {
        try {
            pageOne = pc.getPage(1);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return PageOne.checkVc(pageOne);
    }

    /** 初始化pageIndex
     * 在 DataManager 被创建时，需要获取所有页面并填充 PageIndex。
     * 注意在使用完 Page 后需要及时 release，否则可能会撑爆缓存。
     */
    void fillPageIndex() {
        int pageNumber = pc.getPageNumber();
        for(int i = 2; i <= pageNumber; i ++) {
            Page pg = null;
            try {
                pg = pc.getPage(i);
            } catch (Exception e) {
                Panic.panic(e);
            }
            pIndex.add(pg.getPageNumber(), PageX.getFreeSpace(pg));
            pg.release();   // 注意在使用完 Page 后需要及时 release，否则可能会撑爆缓存
        }
    }
    
}
