package top.guoziyang.mydb.backend.common;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.common.Error;

/**
 * AbstractCache 抽象类， 实现了一个引用计数策略的缓存
 *
 * 一个简单的缓存框架，其他的缓存只需要继承这个类，并实现那两个抽象方法即可
 *
 *  提供了三个方法：
 *       get(long key): 从缓存中获取key资源，并维护一个缓存器；
 *       release(long key): 释放key缓存，依赖 releaseForCache() 方法将缓存写回数据源
 *       close():关闭缓存器，依赖release()方法释放所有缓存
 *  定义了两个抽象方法：
 *       releaseForCache(T obj):当资源被驱逐时的写回行为
 *       getForCache(long key):当资源不在缓存时的获取行为
 */
public abstract class AbstractCache<T> {
    // 引用计数
    private HashMap<Long, T> cache;           // key：缓存资源的标签（pgno或者UID），value：实际缓存的数据
    private HashMap<Long, Integer> references;// key：缓存资源的标签，value：资源的引用个数
    private HashMap<Long, Boolean> getting;   // key：缓存资源的标签，value：是否有线程正在从数据源中获取该资源

    private int maxResource;                  // 缓存的最大缓存资源数
    private int count = 0;                    // 缓存中元素的个数
    private Lock lock;

    public AbstractCache(int maxResource) {
        this.maxResource = maxResource;
        cache = new HashMap<>();
        references = new HashMap<>();
        getting = new HashMap<>();
        lock = new ReentrantLock();  // 可重入锁
    }

    /**
     * get() 方法获取资源。
     *   <1> 首先来无限尝试从"缓存"里获取，进入一个死循环
     *      1.1 请求的资源正在被其他线程获取，休眠一会儿，再来尝试获取锁*
     *      1.2 如果资源在缓存中，就可以直接获取并返回了，记得要给资源的引用数 +1。
     *      1.3 尝试获取该资源。如果缓存没满的话，就在 getting 中注册一下，该线程准备从数据源获取资源了。
     *   <2> 资源不在缓存中，从"数据源"获取资源。
     *      2.1 直接调用那个抽象方法 getForCache(
     *      2.2 getting（正在获取的）中删除 key
     *      2.3 cache（实际缓存的）中添加
     *      2.4 references（元素的引用个数）设为1
     * @param key
     * @return
     * @throws Exception
     */
    protected T get(long key) throws Exception {
        // <1> 首先来无限尝试从"缓存"里获取，进入一个死循环
        while(true) {
            lock.lock(); // 加锁*

            // 1.1 请求的资源正在被其他线程获取，休眠一会儿，再来尝试获取锁*
            if(getting.containsKey(key)) {
                lock.unlock();
                try {
                    // 休眠一会儿，再来尝试获取锁*
                    Thread.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    continue;
                }
                continue;
            }

            // 1.2 如果资源在缓存中，就可以直接获取并返回了，同时给资源的引用数 +1。
            if(cache.containsKey(key)) {
                // 资源在缓存中，直接返回
                T obj = cache.get(key);
                references.put(key, references.get(key) + 1); // 资源的引用数 +1
                lock.unlock();
                return obj;
            }

            // 1.3 尝试获取该资源。如果缓存没满的话，就在 getting 中注册一下，该线程准备从数据源获取资源了。
            if(maxResource > 0 && count == maxResource) {
                lock.unlock();
                throw Error.CacheFullException;
            }
            count ++;               // 缓存中元素的个数 +1
            getting.put(key, true); // 在 getting 中注册（添加）
            lock.unlock();
            break;
        }

        // <2> 资源不在缓存中，从"数据源文件"获取资源。
        //     2.1 直接调用那个抽象方法 getForCache() 即可。
        //     2.2 getting（正在获取的）中删除 key
        //     2.3 cache（实际缓存的）中添加
        //     2.4 references（元素的引用个数）设为1
        T obj = null;
        try {
            obj = getForCache(key); // 2.1 调用抽象方法 getForCache()
        } catch(Exception e) {      // 若发生异常则执行。。
            lock.lock();
            count --;
            getting.remove(key);
            lock.unlock();
            throw e;
        }

        lock.lock();
        getting.remove(key);     // 2.2 getting（正在获取的）中删除 key
        cache.put(key, obj);     // 2.3 cache（实际缓存的）中添加
        references.put(key, 1);  // 2.4 references（元素的引用个数）设为1
        lock.unlock();
        
        return obj;
    }

    /**
     * 强行释放一个缓存。
     * 直接从 references 中减 1，如果已经减到 0 了，就可以回源，并且删除缓存中所有相关的结构
     */
    protected void release(long key) {
        lock.lock();
        try {
            int ref = references.get(key)-1; // 引用个数 -1
            if(ref == 0) {
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
                count --;
            } else {
                references.put(key, ref);   // 当缓存资源被驱逐时的写回行为
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 关闭缓存，写回所有资源,其实就是将所有缓存释放掉
     */
    protected void close() {
        lock.lock();
        try {
            Set<Long> keys = cache.keySet();
            for (long key : keys) {
                T obj = cache.get(key);
                releaseForCache(obj);   // 调用释放缓存方法
                references.remove(key);
                cache.remove(key);
            }
        } finally {
            lock.unlock();
        }
    }


    // 以下两个为抽象方法，留给具体的实现类去完成
    /**
     * 当资源不在缓存时的获取行为
     */
    protected abstract T getForCache(long key) throws Exception;
    /**
     * 当资源被驱逐时的写回行为
     */
    protected abstract void releaseForCache(T obj);
}
