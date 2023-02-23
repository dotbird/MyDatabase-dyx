package top.guoziyang.mydb.backend.dm.pageIndex;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import top.guoziyang.mydb.backend.dm.pageCache.PageCache;

import javax.xml.transform.Source;

/**
 * 页面索引，缓存了每一页的空闲空间。用于在上层模块进行插入操作时，能够快速找到一个合适空间的页面，
 * 而无需从磁盘或者缓存中检查每一个页面的信息。
 *
 * MYDB 用一个比较粗略的算法实现了页面索引，将一页的空间划分成了 40 个区间。
 * 在启动时，就会遍历所有的页面信息，获取页面的空闲空间，安排到这 40 个区间中。
 * 在 insert 请求一个页时，会首先将所需的空间向上取整，映射到某一个区间，随后取出这个区间的任何一页，都可以满足需求
 */
public class PageIndex {
    // 将一页划成40个区间
    private static final int INTERVALS_NO = 40;
    //                 每个区间的大小(阈值) = 每页的大小(8k) / 每页划分的区间数(40)
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO; // 8K / 40 = threshold

    private Lock lock;
    private List<PageInfo>[] lists;   // PageIndex 的实现也很简单，一个 List 类型的数组

    @SuppressWarnings("unchecked")
    public PageIndex() {
        lock = new ReentrantLock();
        lists = new List[INTERVALS_NO+1];     // list 大小为 41
        for (int i = 0; i < INTERVALS_NO+1; i ++) {
            lists[i] = new ArrayList<>();
        }
    }

    /**
     * 将区间插入页面
     * 上层模块使用完这个页面后，需要将其重新插入 PageIndex
     * @param pgno
     * @param freeSpace
     */
    public void add(int pgno, int freeSpace) {
        lock.lock();
        try {
            int number = freeSpace / THRESHOLD;               // 要占用区间数
            lists[number].add(new PageInfo(pgno, freeSpace)); // 插入页面中
        } finally {
            lock.unlock();
        }
    }

    /**
     * 获取页面
     * 从 PageIndex 中获取页面: 算出区间号; 从lists中删除此区间，并返回此区间（获取到了）
     *
     * 注意到:
     * 被选择的页，会直接从 PageIndex 中移除，这意味着，同一个页面是不允许并发写的。
     * 在上层模块使用完这个页面后，需要将其重新插入 PageIndex
     *
     * @param spaceSize
     * @return 获取到的页面信息 PageInfo(页号,空闲空间大小)
     */
    public PageInfo select(int spaceSize) {
        lock.lock();
        try {
            int number = spaceSize / THRESHOLD;    // 算出区间号
            if(number < INTERVALS_NO) number ++;
            while(number <= INTERVALS_NO) {
                if(lists[number].size() == 0) {    // 若此区间长度为0，看下一个
                    number ++;
                    continue;
                }
                // 从lists中删除此区间，并返回此区间（获取到了）**
                return lists[number].remove(0);
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

}
