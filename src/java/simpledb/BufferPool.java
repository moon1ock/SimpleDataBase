package simpledb;

import java.io.*;
import java.util.ArrayList;
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
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    private int numPages;

    private ConcurrentHashMap<PageId, Page> pages;

    private int evitcounter = 0;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
	this.numPages = numPages;
	this.pages = new ConcurrentHashMap<PageId, Page>(numPages);
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
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
	if (this.pages.containsKey(pid)) {
	    return this.pages.get(pid);
	}
	else {
	    // Eviction is not implemented; throw exception for now.
        if (this.pages.size() >= this.numPages)
            this.evictPage();
            // EVIT A PAGE ANDRIY
            // was throwing a DBexception here before
		Page p = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
		this.pages.put(pid, p);
		return p;
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
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1

        // luckily, ConcurrentHashMap supports full concurrency of retrievals
        // so here it will serve us as cache
        ArrayList<Page> newpages = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
        for (Page page : newpages){
            page.markDirty(true, tid);
            if(pages.containsKey(page.getId())) // update the cache if it already contains the page
                pages.put(page.getId(), page);
            else if(this.pages.size() < this.numPages) // if cache not full, add another page to it
                pages.put(page.getId(), page);
            else{ // if it's full, evict first, and then add page
                this.evictPage();
                pages.put(page.getId(), page);
            }
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1

        //delete the tuple
        ArrayList<Page> deletedpages = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId()).deleteTuple(tid, t);

        // check if deletion completed
        if(deletedpages.isEmpty())
            throw new DbException("no tuple was deleted");

        // mark pages dirty and update cache
        for (Page page : deletedpages){
            page.markDirty(true, tid);
            // update cache
            if(pages.containsKey(page.getId())) // update the cache if it already contains the page
                pages.put(page.getId(), page);
            else if(this.pages.size() < this.numPages) // if cache not full, add another page to it
                pages.put(page.getId(), page);
            else{ // if it's full, evict first, and then add page
                this.evictPage();
                pages.put(page.getId(), page);
            }
        }

    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for(PageId id : this.pages.keySet()){
            if (this.pages.get(id).isDirty() != null)
                this.flushPage(id);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.

        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        this.pages.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1

        if(!pages.containsKey(pid))
            throw new IOException("page pid was not found, cannot flush!");
        //write the page to the file
        Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(pages.get(pid));
        //unmark dirty tag
        pages.get(pid).markDirty(false, null);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1


        // for evicting the page, we have implemented a simple
        // evict counter that will iterate over pages one by one
        // and remove them in order. Meaning, the first time,
        // page 1 will be removed, the second time 2 and so on.
        try{
            synchronized(this.pages){
            // allow only 1 thread so that no data is added to the
            //page while it's being flushed, otherwise, might come
            // across reading/writing errors
            this.evitcounter = (this.evitcounter+1)%this.pages.size(); // take modulo if counter is bigger than the buffer pool size
            ArrayList<PageId> pagestoevict = new ArrayList<PageId>(this.pages.keySet()); // creating an array of all pages since cannot access by index in hashmap
            PageId evictedpage = pagestoevict.get(this.evitcounter);
            this.flushPage(evictedpage);
            this.pages.remove(evictedpage);
            //
            }
        } catch(IOException flush){
            throw new DbException("Page could not be flushed while evicting");
        }
        return;
    }

}
