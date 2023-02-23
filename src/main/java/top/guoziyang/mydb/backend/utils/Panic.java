package top.guoziyang.mydb.backend.utils;

/**
 * 强制停机
 */
public class Panic {
    public static void panic(Exception err) {
        err.printStackTrace();
        System.exit(1);
    }
}
