package cn.edu.thssdb.schema;

import java.util.HashSet;
import java.util.Set;

public class SessionLock {
    private final Set<Long> XLockSession;
    private final Set<Long> SLockSessions;

    public SessionLock() {
        XLockSession = new HashSet<>();
        SLockSessions = new HashSet<>();
    }

    public synchronized void XAcquire(long id) throws InterruptedException {
        // 已有X时while条件不满足，会被跳过
        while (!canXAcquire(id)) wait(); // wait will release synchronized lock, safe!
        // while exit case:
        // 0: 无锁, X=S={}
        // 1: 已有X, X={id}, S={}
        // 2: 独占S, X={}, S={id}
        XLockSession.add(id); // 已有X时无效果，已有S时持有双锁
    }

    public synchronized void SAcquire(long id) throws InterruptedException {
        // 已有X时while条件不满足，会被跳过
        while (!canSAcquire(id)) wait(); // wait will release synchronized lock, safe!
        SLockSessions.add(id); // 已有S时无效果，已有X时持有双锁s
    }

    public synchronized void XRelease(long id) {
        XLockSession.remove(id);
        notifyAll();
    }

    public synchronized void SRelease(long id) {
        SLockSessions.remove(id);
        notifyAll();
    }

    public synchronized void Release(long id) {
        XLockSession.remove(id);
        SLockSessions.remove(id);
        notifyAll();
    }

    public synchronized boolean canXAcquire(long id) {
        return setIsEmptyExclude(XLockSession, id) && setIsEmptyExclude(SLockSessions, id);
    }

    public synchronized boolean canSAcquire(long id) {
        return setIsEmptyExclude(XLockSession, id);
    }

    private static boolean setIsEmptyExclude(Set<Long> set, long id) {
        return set.isEmpty() || (set.size() == 1 && set.contains(id));
    }
}
