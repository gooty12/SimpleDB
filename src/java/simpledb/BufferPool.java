package simpledb;

import java.io.*;
import java.util.*;
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
    private static final int PAGE_SIZE = 4096;
    private final LockManager lockManager;
    private static int pageSize = PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;
    
    private static int numPages;
    
    private ConcurrentHashMap<PageId, Page> pages = new ConcurrentHashMap<>();

    private ConcurrentHashMap<PageId, Integer> lruOfPage = new ConcurrentHashMap<>();
    
    private final Map<TransactionId,Set<PageId>> transactionToModifiedPagesMap;

    private int recentValue = 0;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
    	BufferPool.numPages = numPages;
    	lockManager = LockManager.getSingletonInstance();
    	transactionToModifiedPagesMap = new HashMap<TransactionId,Set<PageId>>();
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
    	BufferPool.pageSize = PAGE_SIZE;
    }
    
    private Set<PageId> getAffectedPages(TransactionId tid) {
        if(transactionToModifiedPagesMap.containsKey(tid))
        	return transactionToModifiedPagesMap.get(tid);
        
        transactionToModifiedPagesMap.put(tid, new HashSet<PageId>());
    	return transactionToModifiedPagesMap.get(tid);
      }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
    	lockManager.getLock(tid, pid, perm);
    	
    	Page page = pages.get(pid);
    	if (page == null) {

    		DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
			page = dbFile.readPage(pid);

			if (pages.size() == numPages) {
    			
				this.evictPage();
				this.addPageToCache(pid, page);
    		}
			this.addPageToCache(pid, page);
    	}
    	this.lruOfPage.put(pid, this.recentValue + 1);
    	this.recentValue ++;
    	
        return page;
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
        // not necessary for lab1|lab2|lab3|lab4
    	lockManager.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2|lab3|lab4
    	transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2|lab3|lab4
        return lockManager.doesItHaveLock(tid, p);
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
    	
    	if (lockManager.getAffectedPagesList(tid) == null) return;//Nothing to flush
            
    	Set<PageId> pageIds = lockManager.getAffectedPagesList(tid);
        if (commit) {
        	for (PageId pageId: pageIds)
                    flushPage(pageId);
        } else {
            for (PageId pageId: pageIds)
                    discardPage(pageId);
        }
        
        lockManager.releaseAllLocksOfTransaction(tid);
       }
    
    private void addPageToCache(PageId pid, Page page) {
    	this.pages.put(pid, page);
    	this.recentValue += 1;
    	Integer recentVal = new Integer(recentValue);
    	
    	this.lruOfPage.put(pid, recentVal);
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
        	
    		DbFile dbfile = Database.getCatalog().getDatabaseFile(tableId);
    		ArrayList<Page> modified = dbfile.insertTuple(tid, t);
    		for(int i = 0 ; i < modified.size();i++) {
    			modified.get(i).markDirty(true, tid);
    			this.addPageToCache(modified.get(i).getId(), modified.get(i));
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
    	Catalog dbCatalog = Database.getCatalog();
    	DbFile dFile =	dbCatalog.getDatabaseFile(t.getRecordId().getPageId().getTableId());
    	ArrayList<Page> modified = dFile.deleteTuple(tid, t);
        
    	for(int i = 0 ; i < modified.size();i++) {
			modified.get(i).markDirty(true, tid);
			this.addPageToCache(modified.get(i).getId(), modified.get(i));
		}
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        
		for (PageId key : this.pages.keySet()) {
			this.flushPage(key);
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
    	pages.remove(pid);
        lruOfPage.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
    	if(!this.pages.containsKey(pid))
    		return;
    	if(this.pages.get(pid) == null)
    		return;
        Page page = this.pages.get(pid);
        if(page.isDirty() != null) {
        	DbFile dbFile = Database.getCatalog().getDatabaseFile(pid.getTableId());
			dbFile.writePage(page);
			page.markDirty(false, null);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2|lab3|lab4
    	for(PageId pid:getAffectedPages(tid))
    	{
    		flushPage(pid);
    	}
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage2() throws DbException {
    	PageId keyOfLeastRecent = null;
    	int minimumValue = 1000000;
    	
    	for (PageId key : this.pages.keySet()) {
			
			if(this.lruOfPage.get(key).intValue() < minimumValue) {
				keyOfLeastRecent = key;
				minimumValue = this.lruOfPage.get(key).intValue();
			}
		}
    	try {
			if(this.pages.get(keyOfLeastRecent).isDirty() != null)
				this.flushPage(keyOfLeastRecent);
			pages.remove(keyOfLeastRecent);
			lruOfPage.remove(keyOfLeastRecent);
				
		} 
    	catch (IOException e) {
			e.printStackTrace();
		}
    }
    
    private synchronized  void evictPage() throws DbException {
        for (PageId pid : this.pages.keySet()) {
            Page   p   = pages.get(pid);
            if (p.isDirty() == null) {                
                discardPage(pid);
                return;
            }
        }
        throw new DbException("No Dirty page found");
    }
}
