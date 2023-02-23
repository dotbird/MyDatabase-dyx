package top.guoziyang.mydb.backend.dm;

import top.guoziyang.mydb.backend.dm.dataItem.DataItem;
import top.guoziyang.mydb.backend.dm.logger.Logger;
import top.guoziyang.mydb.backend.dm.page.PageOne;
import top.guoziyang.mydb.backend.dm.pageCache.PageCache;
import top.guoziyang.mydb.backend.tm.TransactionManager;

/**
 * DataManager 是 DM 层直接对外提供方法的类，同时，也实现成 DataItem 对象的缓存。
 * DataItem 存储的 key，是由页号和页内偏移组成的一个 8 字节无符号整数，页号和偏移各占 4 字节。
 *
 * 从已有文件创建 DataManager 和从空文件创建 DataManager 的流程稍有不同
 * DM 层提供了三个功能供上层使用，分别是读、插入和修改。
 * 修改是通过读出的 DataItem 实现的，于是 DataManager 只需要提供 read() 和 insert() 方法。
 */
public interface DataManager {
    DataItem read(long uid) throws Exception;
    long insert(long xid, byte[] data) throws Exception;
    void close();

    /**
     * 从空文件创建 DataManager
     * PageCache 和 Logger 的创建方式有所不同以外，从空文件创建首先需要对第一页进行初始化
     */
    public static DataManager create(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.create(path, mem); // 创建缓存
        Logger lg = Logger.create(path);            // 创建日志

        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm); // 创建 DataManager *
        dm.initPageOne();                           // 对第一页进行初始化
        return dm;
    }

    /**
     * 从已有文件创建 DataManager
     * 需要对第一页进行校验，来判断是否需要执行恢复流程；并重新对第一页生成随机字节。
     */
    public static DataManager open(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.open(path, mem);    // 取出缓存
        Logger lg = Logger.open(path);               // 取出日志
        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm); // 创建 DataManager *
        if(!dm.loadCheckPageOne()) {                 // 对第一页进行校验
            Recover.recover(tm, lg, pc);             // 若校验失败，执行恢复流程
        }
        dm.fillPageIndex();                          // 初始化pageIndex
        PageOne.setVcOpen(dm.pageOne);               // 重新设置 有效检查初始字节--100~107字节
        dm.pc.flushPage(dm.pageOne);                 // 刷新当前页到磁盘文件

        return dm;
    }
}
