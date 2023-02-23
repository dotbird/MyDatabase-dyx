package top.guoziyang.mydb.backend.vm;

import top.guoziyang.mydb.backend.tm.TransactionManager;

/**
 * MVCC的实现代码：实现了读已提交 和 可重复读 两个事务隔离级别
 */
public class Visibility {

    /**
     * 判断是否存在版本跳跃
     */
    public static boolean isVersionSkip(TransactionManager tm, Transaction t, Entry e) {
        long xmax = e.getXmax();
        // 如果事务隔离等级是 读已提交 0，就允许版本跳跃
        if(t.level == 0) {
            return false;
        } else { // 可重复读 1 不允许版本跳跃
            // 已提交删除当前事务版本 并且 这个删除的事务id是在此事务之后发生 或者 是一个未提交的活跃事务操作删除的，就是版本跳跃
            return tm.isCommitted(xmax) && (xmax > t.xid || t.isInSnapshot(xmax));
        }
    }

    /**
     * 当前记录版本对事务的可见性
     */
    public static boolean isVisible(TransactionManager tm, Transaction t, Entry e) {
        if(t.level == 0) {
            return readCommitted(tm, t, e);
        } else {
            return repeatableRead(tm, t, e);
        }
    }

    /**
     * 事务隔离级别：读已提交（一次事务中可能两次读取同一值不一样）
     *
     * 读事务t操作的数据将版本，只要没被删除都是可见的；
     * 读其他事务操作的数据版本，要么是已经提交且未删除的，要么是非自己事务进行删除且未提交的，也是对当前事务t可见的
     *
     * 允许读取并发事务已经提交的数据，可以阻止脏读，但是幻读或不可重复读仍有可能发生。
     * @param tm 事务管理器
     * @param t 事务
     * @param e 数据版本链
     * @return 对事务t的可见性
     */
    private static boolean readCommitted(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;                   // 获取事务id
        long xmin = e.getXmin();            // 获取数据版本链最新版本的操作事务id
        long xmax = e.getXmax();            // 获取数据版本链最新版本的删除事务（下一个事务）id
        // 当前数据版本是事务t创建的，并且没有被删除，则对事务t可见
        if(xmin == xid && xmax == 0) return true;

        // 由一个已经提交的事务创建
        if(tm.isCommitted(xmin)) {
            // 如果没有被删除，则对事务t可见
            if(xmax == 0) return true;
            // 如果由一个未提交的事务删除当前版本，也对事务t可见
            if(xmax != xid) {
                if(!tm.isCommitted(xmax)) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * 事务隔离级别：可重复读（一次事务中两次读取同一值始终一样）
     *
     * 多了一个【记录活跃事务】，简而言之活跃事务操作的数据版本都是不可见的
     * 读取事务t操作的版本只要没被删除都是可见的；
     * 读取其他事务操作过的版本数据，只能读取在本事务开始前就已经提交的事务，并且没有在活跃事务列表里面也没有被删除
     *
     * 事务只能读取它开始时, 就已经结束的那些事务产生的数据版本。（需要一个快照）
     * 对同一字段的多次读取结果都是一致的，除非数据是被本身事务自己所修改，可以阻止脏读和不可重复读，但幻读仍有可能发生。
     *
     * @param tm 事务管理器
     * @param t 事务
     * @param e 数据版本链
     * @return 对事务t的可见性
     */
    private static boolean repeatableRead(TransactionManager tm, Transaction t, Entry e) {
        long xid = t.xid;               // 获取事务id
        long xmin = e.getXmin();        // 获取数据版本链最新版本的操作事务id
        long xmax = e.getXmax();        // 获取数据版本链最新版本的删除事务（下一个事务）id
        // 读取自己操作的版本只要没被删除都是可见的
        if(xmin == xid && xmax == 0) return true;

        // 大范围，只能读取在本事务开始前就已经提交的事务，并且没有在活跃事务列表里面
        if(tm.isCommitted(xmin) && xmin < xid && !t.isInSnapshot(xmin)) {
            // 当前版本还不能被删除
            if(xmax == 0) return true;
            // 删除的事务在本事务之后开始，或者未提交，再或者是活跃事务也是对当前事务可见的
            if(xmax != xid) {
                if(!tm.isCommitted(xmax) || xmax > xid || t.isInSnapshot(xmax)) {
                    return true;
                }
            }
        }
        return false;
    }

}
