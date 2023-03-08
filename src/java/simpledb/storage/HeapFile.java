package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @author Sam Madden
 * @see HeapPage#HeapPage
 */
public class HeapFile implements DbFile {

    private final File file;
    private final TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap
     *          file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        try (RandomAccessFile rFile = new RandomAccessFile(this.file, "rw")) {
            byte[] pageData = new byte[BufferPool.getPageSize()];
            rFile.seek((long) pid.getPageNumber() * BufferPool.getPageSize());
            if (rFile.read(pageData) != -1) {
                return new HeapPage((HeapPageId) pid, pageData);
            }else {
                HeapPage page = new HeapPage((HeapPageId) pid, pageData);
                this.writePage(page);
                return page;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        //todo 后续修改
        try (RandomAccessFile rFile = new RandomAccessFile(this.file, "rw")) {
            rFile.seek((long) page.getId().getPageNumber() * BufferPool.getPageSize());
            rFile.write(page.getPageData());
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) file.length() / BufferPool.getPageSize();
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        List<Page> res = new ArrayList<>();
        int pgNo = 0;
        while (true) {
            HeapPageId pageId = new HeapPageId(this.getId(), pgNo);
            HeapPage page;
//            if (pgNo < this.numPages()) {
            page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
            if (page.getNumEmptySlots() > 0) {
                page.insertTuple(t);
                page.markDirty(true,tid);
                res.add(page);
                break;
            } else {
                Database.getBufferPool().unsafeReleasePage(tid,pageId);
                pgNo++;
            }
//            } else {
//                page = new HeapPage(pageId,new byte[BufferPool.getPageSize()]);
//                this.writePage(page);
//                page.insertTuple(t);
//                break;
//            }
        }
        return res;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public List<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);

        page.deleteTuple(t);
        page.markDirty(true,tid);
        return Collections.singletonList(page);

        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileFileIterator(this, tid);
    }
}

class HeapFileFileIterator extends AbstractDbFileIterator {

    private final HeapFile hFile;
    private final TransactionId tid;

    private HeapPage heapPage;
    private Iterator<Tuple> it = null;

    private int pageNo = 0;

    public HeapFileFileIterator(HeapFile file, TransactionId tid) {
        this.hFile = file;
        this.tid = tid;
    }

    @Override
    protected Tuple readNext() throws DbException, TransactionAbortedException {
        if (it == null) {
            return null;
        }
        while (!it.hasNext()) {
            pageNo++;
            if (this.hFile.numPages() > pageNo) {
                HeapPageId pageId = new HeapPageId(this.hFile.getId(), pageNo);
                this.heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
                this.it = this.heapPage.iterator();
            } else {
                return null;
            }
        }
        return it.next();
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        HeapPageId pageId = new HeapPageId(this.hFile.getId(), pageNo);
        this.heapPage = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_ONLY);
        this.it = this.heapPage.iterator();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        this.close();
        this.open();
    }

    @Override
    public void close() {
        super.close();
        this.it = null;
        this.pageNo = 0;
    }
}
