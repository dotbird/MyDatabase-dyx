package top.guoziyang.mydb.backend.dm.page;

import java.util.Arrays;

import top.guoziyang.mydb.backend.dm.pageCache.PageCache;
import top.guoziyang.mydb.backend.utils.RandomUtil;

/**
 * 特殊管理第一页(只是用来做启动检查的)
 * ValidCheck 有效检查
 * db启动时给100~107字节处填入一个随机字节，db关闭时将其拷贝到108~115字节
 * 用于判断上一次数据库是否正常关闭
 * -数据库在每次启动时，就会检查第一页两处的字节是否相同，以此来判断上一次是否正常关闭。
 * -如果是异常关闭，就需要执行数据的恢复流程。
 */
public class PageOne {
    private static final int OF_VC = 100;
    private static final int LEN_VC = 8;

    public static byte[] InitRaw() {
        byte[] raw = new byte[PageCache.PAGE_SIZE];
        setVcOpen(raw);
        return raw;
    }

    // 启动时设置有效检查初始字节--100~107字节
    public static void setVcOpen(Page pg) {
        pg.setDirty(true);       // 设置为脏页
        setVcOpen(pg.getData()); // 调下面的函数
    }

    private static void setVcOpen(byte[] raw) {
        // 生成一个随机数byte[]数组，拷贝到数组100~107字节处
        // arraycopy（原数组，原数组起始位置，目标数组，目标数组起始位置，拷贝元素的长度）
        System.arraycopy(RandomUtil.randomBytes(LEN_VC), 0, raw, OF_VC, LEN_VC);
    }

    // db关闭时将100~107字节，拷贝到数组108~115字节
    public static void setVcClose(Page pg) {
        pg.setDirty(true);          // 设置为脏页
        setVcClose(pg.getData());
    }

    private static void setVcClose(byte[] raw) {
        // db关闭时将100~107字节，拷贝到数组108~115字节
        System.arraycopy(raw, OF_VC, raw, OF_VC+LEN_VC, LEN_VC);
    }

    // 校验字节：比较两个8字节的数组是否相等，相等返回true
    public static boolean checkVc(Page pg) {
        return checkVc(pg.getData());
    }

    private static boolean checkVc(byte[] raw) {
        // 比较两个8字节的数组是否相等：相等返回true
        return Arrays.equals(
                Arrays.copyOfRange(raw, OF_VC, OF_VC+LEN_VC),                // 100-107
                Arrays.copyOfRange(raw, OF_VC+LEN_VC, OF_VC+2*LEN_VC)); // 108-115
    }
}
