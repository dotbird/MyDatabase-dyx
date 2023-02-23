package top.guoziyang.mydb.backend.vm;

import java.util.HashMap;
import java.util.Map;

import top.guoziyang.mydb.backend.tm.TransactionManagerImpl;

/** vm对其他模块提供的一个抽象的事务数据结构
 * 提供一个结构，来抽象一个事务，以保存快照数据（为可重复读提供快照）
 */
public class Transaction {
    public long xid;                     // 事务id
    public int level;                    // 事务隔离等级，0：读已提交；1：可重复读
    public Map<Long, Boolean> snapshot;  // snapshot：快照。活跃事务的快照，用于实现可重复读
    public Exception err;
    public boolean autoAborted;          // 自动回滚标记

    public static Transaction newTransaction(long xid, int level, Map<Long, Transaction> active) {
        Transaction t = new Transaction();
        t.xid = xid;
        t.level = level;
        // 只有可重复读才需要 活跃事务列表
        if(level != 0) {
            t.snapshot = new HashMap<>();
            for(Long x : active.keySet()) {
                t.snapshot.put(x, true);
            }
        }
        return t;
    }

    // 判断xid是否是活跃事务,活跃事务的快照是否包含
    public boolean isInSnapshot(long xid) {
        if(xid == TransactionManagerImpl.SUPER_XID) {
            return false;
        }
        return snapshot.containsKey(xid);
    }
}
