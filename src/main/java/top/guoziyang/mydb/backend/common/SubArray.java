package top.guoziyang.mydb.backend.common;

/**
 * 实现功能：使两个数组中的一段范围，共用同一片内存，即使这两个数组的长度不同
 *         (实现一个共享内存的子数组)
 *
 * 在 Java 中，当你执行类似 subArray 的操作时，只会在底层进行一个复制，无法同一片内存。
 * 于是，写了一个 SubArray 类，来（松散地）规定这个数组的可使用范围
 */
public class SubArray {
    public byte[] raw;
    public int start;
    public int end;

    public SubArray(byte[] raw, int start, int end) {
        this.raw = raw;
        this.start = start;
        this.end = end;
    }
}
