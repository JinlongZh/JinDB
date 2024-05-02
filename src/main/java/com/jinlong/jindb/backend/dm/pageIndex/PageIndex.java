package com.jinlong.jindb.backend.dm.pageIndex;

import com.jinlong.jindb.backend.dm.pcache.PageCache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * PageIndex 实现了对(pageNo, freeSpace)键值对的缓存.
 * 当DM执行Insert操作时, 可用根据数据大小, 快速的选出有适合空间的页.
 *
 * @Author zjl
 * @Date 2024/5/2
 */
public class PageIndex {

    // 将一页划成40个区间
    private static final int INTERVALS_NO = 40;
    private static final int THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;

    private Lock lock;
    private List<List<Pair>> lists;

    public PageIndex() {
        lock = new ReentrantLock();
        lists = new ArrayList<>(INTERVALS_NO + 1);
        for (int i = 0; i < INTERVALS_NO + 1; i++) {
            lists.add(new ArrayList<>());
        }
    }

    public void add(int pageNo, int freeSpace) {
        lock.lock();
        try {
            int number = freeSpace / THRESHOLD;
            lists.get(number).add(new Pair(pageNo, freeSpace));
        } finally {
            lock.unlock();
        }
    }

    public Pair select(int spaceSize) {
        lock.lock();
        try {
            int number = spaceSize / THRESHOLD;
            if (number < INTERVALS_NO) {
                number++;
            }
            while (number <= INTERVALS_NO) {
                List<Pair> list = lists.get(number);
                if (list.isEmpty()) {
                    number++;
                    continue;
                }
                return list.remove(0);
            }
        } finally {
            lock.unlock();
        }
        return null;
    }

}
