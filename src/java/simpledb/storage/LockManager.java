package simpledb.storage;

import simpledb.common.Permissions;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class LockManager {

    public final Map<TransactionId, Set<PageId>> tpMap = new ConcurrentHashMap<>();

    public final Map<PageId, PageLock> ptMap = new ConcurrentHashMap<>();

    // 考虑读写锁
    public synchronized boolean tryLock(TransactionId tId, PageId pageId, Permissions permissions) {
        PageLock newLock = new PageLock(pageId);
        PageLock pageLock = ptMap.putIfAbsent(pageId, newLock);
        if (pageLock == null) {
            pageLock = newLock;
        }
        Set<PageId> holdLockList = new HashSet<>();
        Set<PageId> oldList = tpMap.putIfAbsent(tId, holdLockList);
        if (oldList != null) {
            holdLockList = oldList;
        }

        boolean res;

        if (Objects.equals(Permissions.READ_ONLY, permissions)) {
            res = pageLock.sharedLock(tId);
        } else {
            res = pageLock.exclusiveLock(tId);
        }
        if (res) {
            holdLockList.add(pageId);
        }
        return res;
    }

    public Collection<PageId> pageIds(TransactionId tid){
        return tpMap.getOrDefault(tid,new HashSet<>());
    }

    public void releaseTransactionLock(TransactionId tid){
        Set<PageId> lockSet = this.tpMap.get(tid);
        if (lockSet!=null){
            for(PageId pageId : lockSet){
                PageLock pageLock = ptMap.get(pageId);
                pageLock.releaseLock(tid);
            }
        }
    }

    public Set<PageId> getLockPageId(TransactionId tid){
        return tpMap.get(tid);
    }


    public Set<PageId> getExclusiveLockPages(TransactionId tid){
        Set<PageId> set =  tpMap.get(tid);
       return set.stream().filter(pageId -> ptMap.get(pageId).current!=null).collect(Collectors.toSet());
    }

    public void releaseLock(TransactionId tid, PageId pid) {
        PageLock pageLock = ptMap.get(pid);
        pageLock.releaseLock(tid);
        tpMap.get(tid).remove(pid);
    }


    public boolean checkExclusiveLock(PageId pageId){
        PageLock pageLock = this.ptMap.getOrDefault(pageId,new PageLock(pageId));
        return pageLock.current == null;

    }

    public boolean holdsLock(TransactionId tid, PageId p) {
        if (tpMap.containsKey(tid)){
            return tpMap.get(tid).contains(p);
        }

        return false;
    }

    static class PageLock {

        private final  PageId pageId;

        private final Set<TransactionId> shares;

        private volatile TransactionId current;

        public PageLock(PageId pageId) {
            this.pageId = pageId;
            this.shares = new HashSet<>();
        }

        public boolean sharedLock(TransactionId tid) {
            if (current == null) {
                shares.add(tid);
                return true;
            }
            return Objects.equals(tid, current);
        }

        public boolean exclusiveLock(TransactionId tid) {
            if (current == null && shares.size() == 0) {
                current = tid;
                return true;
            }

            if (current == null && shares.size()==1 && shares.contains(tid)){
                shares.clear();
                current = tid;
                return true;
            }
            return Objects.equals(tid, current);
        }

        public synchronized void releaseLock(TransactionId tid) {
            if (current == null) {
                shares.remove(tid);
            }else if (Objects.equals(tid, current)) {
                current = null;
            }
        }

        @Override
        public String toString() {
            if (this.current!=null){
                return this.pageId.toString()+this.current.getId();
            }else {
                return this.pageId.toString();
            }
        }
    }

}

