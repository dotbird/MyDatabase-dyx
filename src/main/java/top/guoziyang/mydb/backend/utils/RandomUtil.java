package top.guoziyang.mydb.backend.utils;

import java.security.SecureRandom;
import java.util.Random;

public class RandomUtil {
    // 生成一个随机数byte[]数组
    public static byte[] randomBytes(int length) {
        // SecureRandom：构造一个安全随机数生成器(RNG)，实现默认的随机数算法
        Random r = new SecureRandom();
        byte[] buf = new byte[length];
        r.nextBytes(buf);
        return buf;
    }
}
