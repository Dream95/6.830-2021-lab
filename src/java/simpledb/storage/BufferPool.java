package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    private final int numPages;

    private final Map<PageId, Page> pageMap;

    private final LockManager lockManager = new LockManager();

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages = numPages;
        pageMap = new ConcurrentHashMap<>(numPages);
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // some code goes here
        Page page;
        // 先获取页还是先加锁
        // 先获取页会有数据不一致的问题
        // 需要一个队列记录最后一次使用的
        long start = System.currentTimeMillis();
        long timeOut = (long) (200 + Math.random()* 1000L);

        this.lockManager.ptMap.get(pid);

        while (!this.lockManager.tryLock(tid, pid, perm)){
            if (System.currentTimeMillis()-start>timeOut){
                throw new TransactionAbortedException();
            }
        }


        if (pageMap.containsKey(pid)) {
            return this.pageMap.get(pid);
        } else {
            DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
            page = dbFile.readPage(pid);
            if (this.pageMap.size() >= numPages) {
                synchronized (this.pageMap){
                    this.evictPage();

                }
            }
            this.pageMap.putIfAbsent(pid, page);
            return page;
        }

    }


    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        this.lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2

        this.transactionComplete(tid,true);
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lockManager.holdsLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        Collection<PageId> collections = lockManager.pageIds(tid);

        synchronized (this.pageMap){
            if (commit) {
                collections.forEach(pageId -> {
                    if (this.pageMap.containsKey(pageId) &&  Objects.equals(this.pageMap.get(pageId).isDirty(), tid)) {
                        try {
                            this.flushPage(pageId);
                            this.pageMap.get(pageId).setBeforeImage();
                            this.pageMap.get(pageId).markDirty(false,null);
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                });
            } else {
                Set<PageId> getExclusiveLockPages = lockManager.getExclusiveLockPages(tid);
                collections.forEach(pageId -> {
                    if (this.pageMap.containsKey(pageId) && getExclusiveLockPages.contains(pageId) ){
                        this.pageMap.remove(pageId);
                    }
                });
            }
        }

        lockManager.releaseTransactionLock(tid);
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        List<Page>  pageList = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
        for (Page page : pageList){
            page.markDirty(true,tid);
            pageMap.put(page.getId(),page);
        }
    }


    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        List<Page> pageList = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId()).deleteTuple(tid, t);
        for (Page page : pageList){
            page.markDirty(true,tid);
            pageMap.put(page.getId(),page);
        }
    }


    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (Map.Entry<PageId, Page> entry : this.pageMap.entrySet()) {
            if (entry.getValue().isDirty() != null) {
                this.flushPage(entry.getKey());
                entry.getValue().setBeforeImage();
                this.pageMap.get(entry.getKey()).markDirty(false,null);
            }
        }

    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1

        /// 是否需要从lock中删除
        pageMap.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page p = this.pageMap.get(pid);
        TransactionId dirtier = p.isDirty();
        if (dirtier != null){
            Database.getLogFile().logWrite(dirtier, p.getBeforeImage(), p);
            Database.getLogFile().force();
        }
        Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(this.pageMap.get(pid));

    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        // 如何保证驱逐时没有锁
        if (this.pageMap.size()< numPages){
            return;
        }

        boolean evictSuccess = false;
        Iterator<Map.Entry<PageId, Page>> iterable = pageMap.entrySet().iterator();
        while (iterable.hasNext()) {
            Map.Entry<PageId, Page> entry = iterable.next();
             if (entry.getValue().isDirty()==null ){
                 iterable.remove();
                 evictSuccess = true;
            }
        }
        if (!evictSuccess){
            throw new DbException("evict Fail");
        }

    }

    public void releaseLock(TransactionId tid,PageId pageId){
        this.lockManager.releaseLock(tid,pageId);
    }
}

