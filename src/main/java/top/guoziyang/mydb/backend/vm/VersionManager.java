package top.guoziyang.mydb.backend.vm;

import top.guoziyang.mydb.backend.dm.DataManager;
import top.guoziyang.mydb.backend.tm.TransactionManager;

/**
 * VM 层通过 VersionManager 接口，向上层提供功能
 */
public interface VersionManager {
    // 数据版本链管理
    byte[] read(long xid, long uid) throws Exception;    // 保证可见性的条件下，读取数据DataItem
    long insert(long xid, byte[] data) throws Exception; // 通过事务xid插入数据
    boolean delete(long xid, long uid) throws Exception; // 通过事务xid删除数据

    // 事务管理
    long begin(int level);                  // 事务开启隔离级别
    void commit(long xid) throws Exception; // 提交事务
    void abort(long xid);                   // 撤销事务

    public static VersionManager newVersionManager(TransactionManager tm, DataManager dm) {
        return new VersionManagerImpl(tm, dm);
    }

}
