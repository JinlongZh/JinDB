package com.jinlong.jindb.backend.vm;

import com.jinlong.jindb.common.ErrorConstants;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 锁表维护了一个有向图，每次添加边的时候，就会进行死锁检测
 *
 * @Author zjl
 * @Date 2024/6/9
 */
public class LockTable {

    /**
     * 某个XID已经获得的资源的UID列表
     */
    private Map<Long, List<Long>> x2u;
    /**
     * UID被某个XID持有(key:uid，value:xid)
     */
    private Map<Long, Long> u2x;
    /**
     * 正在等待UID的XID列表（key：uid，value：xid列表）
     */
    private Map<Long, List<Long>> wait;
    /**
     * 正在等待资源的XID的锁(key:xid)
     */
    private Map<Long, Lock> waitLock;
    /**
     * XID正在等待的UID（key：xid，value：uid）
     */
    private Map<Long, Long> waitU;
    private Lock lock;

    public LockTable() {
        x2u = new HashMap<>();
        u2x = new HashMap<>();
        wait = new HashMap<>();
        waitLock = new HashMap<>();
        waitU = new HashMap<>();
        lock = new ReentrantLock();
    }

    /**
     * 不需要等待则返回null，否则返回锁对象，死锁则抛异常
     */
    public Lock add(long xid, long uid) throws Exception {
        lock.lock();
        try {
            // 如果xid已经包含了uid, 则直接返回true
            if (isInList(x2u, xid, uid)) {
                return null;
            }
            if (!u2x.containsKey(uid)) {
                u2x.put(uid, xid);
                putIntoList(x2u, xid, uid);
                return null;
            }
            waitU.put(xid, uid);
            putIntoList(wait, uid, xid);
            if (hasDeadLock()) {
                waitU.remove(xid);
                removeFromList(wait, uid, xid);
                throw ErrorConstants.DeadlockException;
            }
            Lock l = new ReentrantLock();
            l.lock();
            waitLock.put(xid, l);
            return l;
        } finally {
            lock.unlock();
        }
    }


    public void remove(long xid) {
        lock.lock();
        try {
            List<Long> l = x2u.get(xid);
            if (l != null) {
                while (l.size() > 0) {
                    Long uid = l.remove(0);
                    selectNewXID(uid);
                }
            }
            waitU.remove(xid);
            x2u.remove(xid);
            waitLock.remove(xid);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 从等待队列中选择一个xid来占用uid
     */
    private void selectNewXID(long uid) {
        u2x.remove(uid);
        List<Long> l = wait.get(uid);
        if (l == null) return;
        assert l.size() > 0;

        while (l.size() > 0) {
            long xid = l.remove(0);
            if (!waitLock.containsKey(xid)) {
                continue;
            } else {
                u2x.put(uid, xid);
                Lock lo = waitLock.remove(xid);
                waitU.remove(xid);
                lo.unlock();
                break;
            }
        }

        if (l.size() == 0) wait.remove(uid);
    }


    private Map<Long, Integer> xidStamp;
    private int stamp;

    private boolean hasDeadLock() {
        xidStamp = new HashMap<>();
        stamp = 1;
        for (long xid : x2u.keySet()) {
            Integer s = xidStamp.get(xid);
            // 已经dfs过了
            if (s != null && s > 0) {
                continue;
            }
            stamp++;
            if (dfs(xid)) {
                return true;
            }
        }
        return false;
    }

    private boolean dfs(long xid) {
        Integer stp = xidStamp.get(xid);
        if (stp != null && stp == stamp) {
            return true;
        }
        if (stp != null && stp < stamp) {
            return false;
        }
        xidStamp.put(xid, stamp);

        Long uid = waitU.get(xid);
        if (uid == null) {
            return false;
        }
        Long x = u2x.get(uid);

        assert x != null;

        return dfs(x);
    }

    private void removeFromList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> l = listMap.get(uid0);
        if (l == null) return;
        Iterator<Long> i = l.iterator();
        while (i.hasNext()) {
            long e = i.next();
            if (e == uid1) {
                i.remove();
                break;
            }
        }
        if (l.size() == 0) {
            listMap.remove(uid0);
        }
    }

    private void putIntoList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        if (!listMap.containsKey(uid0)) {
            listMap.put(uid0, new ArrayList<>());
        }
        listMap.get(uid0).add(0, uid1);
    }

    private boolean isInList(Map<Long, List<Long>> listMap, long uid0, long uid1) {
        List<Long> l = listMap.get(uid0);
        if (l == null) return false;
        Iterator<Long> i = l.iterator();
        while (i.hasNext()) {
            long e = i.next();
            if (e == uid1) {
                return true;
            }
        }
        return false;
    }


}
