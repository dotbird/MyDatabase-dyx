package top.guoziyang.mydb.backend.dm.page;

import java.util.Arrays;

import top.guoziyang.mydb.backend.dm.pageCache.PageCache;
import top.guoziyang.mydb.backend.utils.Parser;

/**
 * PageX管理普通页
 * 普通页结构
 * [FreeSpaceOffset] [Data] ----【前两字节为Date.length，后面字节为Date】
 * FreeSpaceOffset: 2字节 空闲位置开始偏移
 * - 前 2 字节，表示这一页已经用了多少了，也就是后面数组的长度（空闲位置的偏移）。
 * - 后面字节，才是存储实际的数据。
 * - 所以对普通页的管理，基本都是围绕着对 空闲空间偏移量 FSO（Free Space Offset）进行的
 */
public class PageX {
    
    private static final short OF_FREE = 0;
    private static final short OF_DATA = 2;  // 以一个 2 字节无符号数起始
    public static final int MAX_FREE_SPACE = PageCache.PAGE_SIZE - OF_DATA; //一页的最大可用空间 8k - 2

    public static byte[] initRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setFSO(raw, OF_DATA);
        return raw;
    }

    /**
     * 设置 FSO(空闲空间偏移量)，也就是后面数据的长度，已经使用的长度，接下来要从那个下标开始插入
     * @param raw
     * @param ofData
     */
    private static void setFSO(byte[] raw, short ofData) {
        // 把 ofDate 插入到 raw 的 0-2 位
        System.arraycopy(Parser.short2Byte(ofData), 0, raw, OF_FREE, OF_DATA);
    }

    // 获取 pg 的 FSO(空闲空间偏移量)，也就是后面数据的长度
    public static short getFSO(Page pg) {
        return getFSO(pg.getData());
    }

    // 拷贝 raw 前两位为一个新数组，并返回 新数组长度(FSO空闲空间偏移量)
    private static short getFSO(byte[] raw) {
        // byte[]类型 -> short类型； 拷贝数组 copyOfRange(原数组，起始位置，终止位置)
        return Parser.parseShort(Arrays.copyOfRange(raw, 0, 2));
    }

    // 获取页面的空闲空间大小
    public static int getFreeSpace(Page pg) {
        // 页面总大小 8K - pg页使用的长度(就是FSO)
        return PageCache.PAGE_SIZE - (int)getFSO(pg.getData());
    }

    /** 将raw插入pg中，返回插入位置(原来的使用长度)
     *
     * @param pg
     * @param raw
     * @return
     */
    public static short insert(Page pg, byte[] raw) {
        pg.setDirty(true);      // 设置为脏页
        short offset = getFSO(pg.getData()); // 拷贝pg前两位为新数组,并返回数组长度
        // 把row插入到pg的第二位往后
        // arraycopy（原数组，原数组起始位置，目标数组，目标数组起始位置，拷贝元素的长度）
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
        // 设置使用长度FSO：把总长度 (offset + raw.length) 插入到 pg 的 0-2 位
        setFSO(pg.getData(), (short)(offset + raw.length));
        return offset;
    }

    /**
     * 在数据库崩溃后重新打开时，恢复例程--直接插入数据
     *   将raw插入pg中的offset位置，并将pg的offset设置为较大的offset
     * @param pg
     * @param raw
     * @param offset
     */
    public static void recoverInsert(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);       // 设置为脏页
        // 把 raw 全部插入到 pg 的 offset 位置后
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);

        // 设置 pg 的使用长度为较大的一个（设置FSO）
        short rawFSO = getFSO(pg.getData());
        if(rawFSO < offset + raw.length) {
            setFSO(pg.getData(), (short)(offset+raw.length));
        }
    }

    /**
     * 在数据库崩溃后重新打开时，恢复例程--修改数据
     *   将raw插入pg中的offset位置，不更新 FSO
     */
    public static void recoverUpdate(Page pg, byte[] raw, short offset) {
        pg.setDirty(true);
        // 把 raw 全部插入到 pg 的 offset 位置后
        System.arraycopy(raw, 0, pg.getData(), offset, raw.length);
    }
}
