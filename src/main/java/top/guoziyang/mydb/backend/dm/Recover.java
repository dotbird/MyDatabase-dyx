package top.guoziyang.mydb.backend.dm;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.primitives.Bytes;

import top.guoziyang.mydb.backend.common.SubArray;
import top.guoziyang.mydb.backend.dm.dataItem.DataItem;
import top.guoziyang.mydb.backend.dm.logger.Logger;
import top.guoziyang.mydb.backend.dm.page.Page;
import top.guoziyang.mydb.backend.dm.page.PageX;
import top.guoziyang.mydb.backend.dm.pageCache.PageCache;
import top.guoziyang.mydb.backend.tm.TransactionManager;
import top.guoziyang.mydb.backend.utils.Panic;
import top.guoziyang.mydb.backend.utils.Parser;

/**
 * 恢复策略
 *  1.在进行 插入 和 修改 操作之前，必须先进行对应的日志操作，在保证日志写入磁盘后，才进行数据操作
 *
 *  修改 updateLog: [LogType] [XID] [UID] [OldRaw] [NewRaw]
 *  插入 insertLog: [LogType] [XID] [Pgno] [Offset] [Raw]
 *
 *  - redo log：重做日志。数据库崩溃之后，根据 redo log 重做恢复原样。
 *              还有个作用：先写到日志里，攒一部分，再刷盘。
 *              日志中是插入，则插入；日志中是修改，则修改。
 *  - undo log：回滚日志。数据库执行中发生异常，根据 undo log 直接回滚恢复原样。
 *              记录的日志是相反的（对应 un）,若增加，它记删除；若修改，它记原值。
 */
public class Recover {

    private static final byte LOG_TYPE_INSERT = 0;  // 日志类型：插入
    private static final byte LOG_TYPE_UPDATE = 1;  // 日志类型：修改

    private static final int REDO = 0; // redo:重做日志
    private static final int UNDO = 1; // undo:回滚日志

    // 静态内部类，插入日志信息
    static class InsertLogInfo {
        long xid;
        int pgno;
        short offset;
        byte[] raw;
    }

    // 静态内部类，修改日志信息
    static class UpdateLogInfo {
        long xid;
        int pgno;
        short offset;
        byte[] oldRaw;
        byte[] newRaw;
    }

    /**
     * 恢复：重做所有已完成事务，撤销所有未完成事务
     *
     * redoTranscations(tm, lg, pc); // 执行 redo: 重做所有已完成事务*
     * undoTranscations(tm, lg, pc); // 执行 undo: 撤销所有未完成事务*
     *
     */
    public static void recover(TransactionManager tm, Logger lg, PageCache pc) {
        System.out.println("Recovering...");

        lg.rewind();                 // log日志倒带，指针指到4
        int maxPgno = 0;
        while(true) {
            byte[] log = lg.next();  // next()迭代下一条
            if(log == null) break;   // 迭代完log；下一句为 null,跳出循环*
            int pgno;                // PageNumber页数
            if(isInsertLog(log)) {   // 若为 插入的日志
                InsertLogInfo li = parseInsertLog(log); // 把log包装为 插入日志信息类
                pgno = li.pgno;      // 页数
            } else {                 // 为 修改的日志
                UpdateLogInfo li = parseUpdateLog(log); // 把log包装为 修改日志信息类
                pgno = li.pgno;      // 页数
            }
            if(pgno > maxPgno) {
                maxPgno = pgno;      // 更新最大页数
            }
        }
        if(maxPgno == 0) {
            maxPgno = 1;
        }
        pc.truncateByBgno(maxPgno);   // 根据页数 截断页面缓存
        System.out.println("Truncate to " + maxPgno + " pages.");

        redoTranscations(tm, lg, pc); // 执行 redo: 重做所有已完成事务*
        System.out.println("Redo Transactions Over.");

        undoTranscations(tm, lg, pc); // 执行 undo: 撤销所有未完成事务*
        System.out.println("Undo Transactions Over.");

        System.out.println("Recovery Over.");
    }

    /**
     * redo事务: 重做所有已完成事务
     */
    private static void redoTranscations(TransactionManager tm, Logger lg, PageCache pc) {
        lg.rewind();                                    // 倒带
        while(true) {
            byte[] log = lg.next();                     // next()迭代下一条
            if(log == null) break;                      // 迭代完log；下一句为 null,跳出循环*
            if(isInsertLog(log)) {                      // 若为插入的日志
                InsertLogInfo li = parseInsertLog(log); // 把log包装为 插入日志信息类
                long xid = li.xid;
                if(!tm.isActive(xid)) {                 // 不是正在进行的事务
                    doInsertLog(pc, log, REDO);         // 恢复时，根据log日志插入*
                }
            } else {                                    // 为修改的日志
                UpdateLogInfo xi = parseUpdateLog(log); // 把log包装为 修改日志信息类
                long xid = xi.xid;
                if(!tm.isActive(xid)) {                 // 不是正在进行的事务
                    doUpdateLog(pc, log, REDO);         // 恢复时，根据log日志修改*
                }
            }
        }
    }

    /**
     * undo事务: 撤销所有未完成事务*
     */
    private static void undoTranscations(TransactionManager tm, Logger lg, PageCache pc) {
        Map<Long, List<byte[]>> logCache = new HashMap<>();    // 日志缓存map
        lg.rewind();                                           // 倒带
        while(true) {
            byte[] log = lg.next();                            // next()迭代下一条
            if(log == null) break;                             // 迭代完log；下一句为 null,跳出循环*
            if(isInsertLog(log)) {                             // 1.若为插入的日志
                InsertLogInfo li = parseInsertLog(log);        // 把log包装为 插入日志信息类
                long xid = li.xid;
                if(tm.isActive(xid)) {                         // 是正在进行的事务(回滚)
                    if(!logCache.containsKey(xid)) {           // 若map中没包含
                        logCache.put(xid, new ArrayList<>());  // 加入日志缓存map*
                    }
                    logCache.get(xid).add(log);                // 若缓存中有，将log加到末尾*
                }
            } else {                                           // 2.若为修改的日志
                UpdateLogInfo xi = parseUpdateLog(log);        // 把log包装为 修改日志信息类
                long xid = xi.xid;
                if(tm.isActive(xid)) {                         // 是正在进行的事务(回滚)
                    if(!logCache.containsKey(xid)) {           // 若map中没包含
                        logCache.put(xid, new ArrayList<>());  // 加入日志缓存map*
                    }
                    logCache.get(xid).add(log);                // 若缓存中有，将log加到末尾*
                }
            }
        }

        // 对所有active log进行倒序 undo 回滚
        for(Entry<Long, List<byte[]>> entry : logCache.entrySet()) {
            List<byte[]> logs = entry.getValue();
            for (int i = logs.size()-1; i >= 0; i --) {  // 倒序
                byte[] log = logs.get(i);
                if(isInsertLog(log)) {                   // 若为 插入的日志
                    doInsertLog(pc, log, UNDO);          // 恢复时，根据log日志插入（undo插入对应删除）*
                } else {                                 // 若为 修改的日志
                    doUpdateLog(pc, log, UNDO);
                }
            }
            tm.abort(entry.getKey());
        }
    }

    // 是否为插入的日志
    private static boolean isInsertLog(byte[] log) {
        return log[0] == LOG_TYPE_INSERT;  // 标志位对比
    }

    // [LogType] [XID] [UID] [OldRaw] [NewRaw]
    private static final int OF_TYPE = 0;
    private static final int OF_XID = OF_TYPE+1;
    private static final int OF_UPDATE_UID = OF_XID+8;
    private static final int OF_UPDATE_RAW = OF_UPDATE_UID+8;

    public static byte[] updateLog(long xid, DataItem di) {
        byte[] logType = {LOG_TYPE_UPDATE};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] uidRaw = Parser.long2Byte(di.getUid());
        byte[] oldRaw = di.getOldRaw();
        SubArray raw = di.getRaw();
        byte[] newRaw = Arrays.copyOfRange(raw.raw, raw.start, raw.end);
        return Bytes.concat(logType, xidRaw, uidRaw, oldRaw, newRaw);
    }

    // 包装为 修改日志信息类: [LogType] [XID] [UID] [OldRaw] [NewRaw]
    //                      0        1-9   9-17   17-..    17-..
    private static UpdateLogInfo parseUpdateLog(byte[] log) {
        UpdateLogInfo li = new UpdateLogInfo();
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_UPDATE_UID));
        long uid = Parser.parseLong(Arrays.copyOfRange(log, OF_UPDATE_UID, OF_UPDATE_RAW));
        li.offset = (short)(uid & ((1L << 16) - 1));    // 偏移量
        uid >>>= 32;
        li.pgno = (int)(uid & ((1L << 32) - 1));        // 页数
        int length = (log.length - OF_UPDATE_RAW) / 2;
        li.oldRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW, OF_UPDATE_RAW+length);
        li.newRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW+length, OF_UPDATE_RAW+length*2);
        return li;
    }

    // 恢复时，根据log日志修改（包括重做和回滚）*
    private static void doUpdateLog(PageCache pc, byte[] log, int flag) {
        int pgno;                                   // 页数
        short offset;                               // 偏移量
        byte[] raw;
        if(flag == REDO) {                          // 1.若为 重做
            UpdateLogInfo xi = parseUpdateLog(log); // 包装为 修改日志信息类
            pgno = xi.pgno;
            offset = xi.offset;
            raw = xi.newRaw;
        } else {                                    // 2.若为 回滚
            UpdateLogInfo xi = parseUpdateLog(log); // 包装为 修改日志信息类
            pgno = xi.pgno;
            offset = xi.offset;
            raw = xi.oldRaw;
        }
        Page pg = null;
        try {
            pg = pc.getPage(pgno);                  // 获取资源（先缓存，再文件）
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            PageX.recoverUpdate(pg, raw, offset);  // 数据库崩溃后重新打开时，恢复例程--[修改数据]
        } finally {
            pg.release();                          // 释放资源
        }
    }

    // [LogType] [XID] [Pgno] [Offset] [Raw]
    private static final int OF_INSERT_PGNO = OF_XID+8;
    private static final int OF_INSERT_OFFSET = OF_INSERT_PGNO+4;
    private static final int OF_INSERT_RAW = OF_INSERT_OFFSET+2;

    public static byte[] insertLog(long xid, Page pg, byte[] raw) {
        byte[] logTypeRaw = {LOG_TYPE_INSERT};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] pgnoRaw = Parser.int2Byte(pg.getPageNumber());
        byte[] offsetRaw = Parser.short2Byte(PageX.getFSO(pg));
        return Bytes.concat(logTypeRaw, xidRaw, pgnoRaw, offsetRaw, raw);
    }

    // 包装为 插入日志信息类：[LogType] [XID] [Pgno] [Offset] [Raw]
    //                      0       1-9    9-13   13-15    15-...
    private static InsertLogInfo parseInsertLog(byte[] log) {
        InsertLogInfo li = new InsertLogInfo();
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_INSERT_PGNO));
        li.pgno = Parser.parseInt(Arrays.copyOfRange(log, OF_INSERT_PGNO, OF_INSERT_OFFSET));
        li.offset = Parser.parseShort(Arrays.copyOfRange(log, OF_INSERT_OFFSET, OF_INSERT_RAW));
        li.raw = Arrays.copyOfRange(log, OF_INSERT_RAW, log.length);
        return li;
    }

    // 恢复时，根据log日志插入（包括重做和回滚）*
    private static void doInsertLog(PageCache pc, byte[] log, int flag) {
        InsertLogInfo li = parseInsertLog(log); // 把log包装为 插入日志信息类
        Page pg = null;
        try {
            pg = pc.getPage(li.pgno);  // 得到页数
        } catch(Exception e) {
            Panic.panic(e);
        }
        try {
            if(flag == UNDO) {     // 若是 undo log 回滚日志，插入的相反就是删除（回滚）
                DataItem.setDataItemRawInvalid(li.raw);  //将该条 DataItem的有效位设置为无效，进行逻辑删除
            }                      // 若是 redo log 重做日志，插入就是插入（重做）
            PageX.recoverInsert(pg, li.raw, li.offset); // 在数据库崩溃后重新打开时，恢复例程--[直接插入数据]
        } finally {
            pg.release();   // 释放缓存
        }
    }
}
