package simpledb;

import java.io.*;
import java.lang.reflect.Array;
import java.nio.file.NoSuchFileException;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {




    private File f;
    private TupleDesc td;
    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.f;
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
        return f.getAbsoluteFile().hashCode();
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

        byte[] content = new byte[BufferPool.getPageSize()];

        try{
        RandomAccessFile read = new RandomAccessFile(this.f, "rw");

        read.seek(BufferPool.getPageSize() * pid.getPageNumber());
        read.read(content);
        HeapPage readpage = new HeapPage ( (HeapPageId) pid, content);
        read.close();
        return readpage;
       }
        catch (NoSuchFileException fnf) {System.out.println( "Error while reading the file" );}
        catch (IOException ioe) {System.out.println( "Error while reading the file" );}

       return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        try{
            RandomAccessFile file = new RandomAccessFile(this.f, "rw");
            file.seek(BufferPool.getPageSize() * page.getId().getPageNumber()); // seek the offset
            file.write(page.getPageData()); // write the byte data to the end of the page
            file.close();
        }catch(IOException e){
            throw new IOException();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) (f.length()/BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1

        ArrayList<Page> newpages = new ArrayList<Page>(); // for marking page as dirty
        boolean flag = false;
        // first let's see if there are any empty slots on the existing pages
        for (int i = 0; i < this.numPages(); i++){
            HeapPage tuplepage = (HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(this.getId(), i), Permissions.READ_WRITE);
            if(tuplepage.getNumEmptySlots() != 0){
                tuplepage.insertTuple(t);
                newpages.add(tuplepage);
                flag = true;
                break;
            }
        }
        // if the flag didn't fall, it means that the page was not added, and we have to create a new one
        if(!flag){
            HeapPageId newid = new HeapPageId(this.getId(), this.numPages());
            HeapPage tuplepage = new HeapPage(newid, HeapPage.createEmptyPageData());
            tuplepage.insertTuple(t);
            newpages.add(tuplepage);
            // write page here
            writePage(tuplepage);
        }


        return newpages;

    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> deleted = new ArrayList<Page>(); // array to keep track of deleted pages to mark them as diry later
    
        HeapPage deletedpage = (HeapPage)Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        deletedpage.deleteTuple(t);
        deleted.add(deletedpage);
        return deleted;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
	return new HeapFileIterator(this, tid);
    }

    public class HeapFileIterator extends AbstractDbFileIterator {
	final HeapFile hf;
	final TransactionId tid;
	Iterator<Tuple> tuples;
	int pcnt;

	HeapFileIterator(HeapFile hf, TransactionId tid) {
	    this.hf = hf;
	    this.tid = tid;
	}

	public Iterator<Tuple> pageIterator(int pno) throws TransactionAbortedException, DbException {
	    HeapPageId pid = new HeapPageId(this.hf.getId(), pno);
	    HeapPage page = (HeapPage)Database.getBufferPool().getPage(this.tid, pid, Permissions.READ_ONLY);
	    return page.iterator();
	};

	public void open() throws TransactionAbortedException, DbException {
	    this.pcnt = 0;
	    if (hf.numPages() > 0)
		this.tuples = pageIterator(pcnt);
	}

	public Tuple readNext() throws TransactionAbortedException, DbException {
	    while (tuples != null && !tuples.hasNext() && pcnt < hf.numPages() - 1) {
		pcnt++;
		tuples = pageIterator(pcnt);
	    }

	    if (tuples != null && tuples.hasNext())
		return tuples.next();

	   return null;
	}

	public void close() {
	    super.close();
	    tuples = null;
	}

	public void rewind() throws TransactionAbortedException, DbException {
	    this.close();
	    this.open();
	}
    }
}

