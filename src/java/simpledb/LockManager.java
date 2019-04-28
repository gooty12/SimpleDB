package simpledb;

import java.util.*;
import java.util.concurrent.*;
public class LockManager {
    private final ConcurrentMap<PageId, Object> pageToLockMap;
    
    private final Map<PageId, Set<TransactionId>> shareLocksMap;
    
    private final Map<PageId, TransactionId> exclusiveLocksMap;
    
    private final Map<TransactionId, Set<PageId>> affectedPages;
    
    private final Map<TransactionId, PageId> intentionLock;
    private static final LockManager instance = new LockManager();
    
    private LockManager()
    {
    	pageToLockMap = new ConcurrentHashMap<PageId, Object>();   
        shareLocksMap = new HashMap<PageId, Set<TransactionId>>();   
        exclusiveLocksMap = new HashMap<PageId, TransactionId>();   
        affectedPages = new HashMap<TransactionId, Set<PageId>>(); 
        intentionLock = new HashMap<TransactionId, PageId>();
    }

    //Return a singleton for the manager
    public static LockManager getSingletonInstance()
    {
    	return new LockManager();
    }
    
    public Set<PageId> getAffectedPagesList(TransactionId tid) {
    	if(affectedPages.containsKey(tid))
    		return affectedPages.get(tid);
    	
    	affectedPages.put(tid, new HashSet<PageId>());
		return affectedPages.get(tid);
	}
    
    private Object getLockOfPage(PageId pageId) {
    	if(pageToLockMap.containsKey(pageId))
    		return pageToLockMap.get(pageId);
    	
    	pageToLockMap.put(pageId, new Object());
		return pageToLockMap.get(pageId);
	}
    
    private Set<TransactionId> getSharedLockList(PageId pageId) {
		if(this.shareLocksMap.containsKey(pageId))
			return shareLocksMap.get(pageId);
		
		shareLocksMap.put(pageId,new HashSet<TransactionId>());
		return shareLocksMap.get(pageId);
	}
    
    public void getLock(TransactionId tid, PageId pid, Permissions permission) throws TransactionAbortedException
    {
        if(permission == Permissions.READ_ONLY)
	        acquireSharedLock(tid,pid);
        
        else
            acquireExclusiveLock(tid,pid);
    }

    public void releaseAllLocksOfTransaction(TransactionId tid)
    {
    	Set<PageId> pageset = new HashSet<PageId>(getAffectedPagesList(tid)); 
    	for(PageId pid: pageset)
    	{
    		releaseLock(tid,pid);
    	}
    }
    
    private void removeXLockFromTransaction(PageId pid, TransactionId tid) {
    	if(exclusiveLocksMap.get(pid) == null)
    		return;
    	if(exclusiveLocksMap.get(pid).equals(tid))
    		exclusiveLocksMap.put(pid, null);
    }
    
    private void removeSLockFromTransaction(PageId pid, TransactionId tid) {
    	Set<TransactionId> currentTransactions = getSharedLockList(pid);
    	currentTransactions.remove(tid);
    	this.shareLocksMap.put(pid, currentTransactions);
    }
    
    private void removeAffectedPagesFromTransaction(PageId pid, TransactionId tid) {
    	Set<PageId> currentPages = getAffectedPagesList(tid);
    	currentPages.remove(pid);
    	this.affectedPages.put(tid, currentPages);
    }
    
    public void releaseLock(TransactionId tid, PageId pid)
    {
    	Object lock = getLockOfPage(pid);
        synchronized(lock)
        {
        	removeXLockFromTransaction(pid, tid);

          
        	removeSLockFromTransaction(pid, tid);
            	
        	removeAffectedPagesFromTransaction(pid, tid);
        	return;
        }
    }
    
    private void updateAffectedPages(PageId pid, TransactionId tid) {
    	Set<PageId> currentPages = getAffectedPagesList(tid);
    	currentPages.add(pid);
    	this.affectedPages.put(tid, currentPages);
    }
    
    private void updateSharedLockList(PageId pid, TransactionId tid) {
    	Set<TransactionId> currentTransactions = getSharedLockList(pid);
		currentTransactions.add(tid);
		this.shareLocksMap.put(pid, currentTransactions);
    }
    public void acquireSharedLock(TransactionId tid, PageId pid) throws TransactionAbortedException
    {
    	Object lock = getLockOfPage(pid);

    	intentionLock.put(tid, pid);
    	deadlockTest(tid,pid,false);

    	//Loop and wait for lock.
        while(true)
        {
            synchronized(lock)
            {
            	
            	//Check if there is no X lock holder for this page.
            	
            	if(exclusiveLocksMap.get(pid) == null) {

            		updateSharedLockList(pid, tid);

            		updateAffectedPages(pid, tid);
                	                	
                	intentionLock.remove(tid);
                	return;
            	}
            	
            	if(exclusiveLocksMap.get(pid) != null)
            		if(!exclusiveLocksMap.get(pid).equals(tid))
            			continue;
            		else//Already has the X lock
            		{

            			intentionLock.remove(tid);
            			return;
            		}
        			

        		updateSharedLockList(pid, tid);


        		updateAffectedPages(pid, tid);
            	                	
            	intentionLock.remove(tid);
            	return;
            }
        }
    }

    public void acquireExclusiveLock(TransactionId tid, PageId pid) throws TransactionAbortedException
    {
    	Object lock = getLockOfPage(pid);
    	intentionLock.put(tid, pid);
    	deadlockTest(tid,pid,true);
    	
    	//Loop and wait for lock.
    	while(true)
        {
            synchronized(lock)
            {
            	
            	if(exclusiveLocksMap.get(pid) != null) {
            		if(!exclusiveLocksMap.get(pid).equals(tid))
            			continue;
            		else
            		{
            			intentionLock.remove(tid);
            			return;
            		}
            	}
            	
            	if(getSharedLockList(pid).isEmpty())
            	{
            		exclusiveLocksMap.put(pid, tid);

            		updateAffectedPages(pid, tid);
            		
            		intentionLock.remove(tid);
            		return;
            	}
            	
            	else if(getSharedLockList(pid).size() == 1 & getSharedLockList(pid).iterator().next().equals(tid))
            	{
            		//update lock
            		getSharedLockList(pid).clear();
            		exclusiveLocksMap.put(pid, tid);
            		
            		updateAffectedPages(pid, tid);
            		
            		intentionLock.remove(tid);
            		return;

            	}    	
            }
        }
    }
    
    public boolean doesItHaveLock(TransactionId tid, PageId pid)
    {
    	return getAffectedPagesList(tid).contains(pid);
    }
    
    public void deadlockTest(TransactionId tid, PageId pid, boolean ifExclusive) throws TransactionAbortedException
    {

    	ArrayList<TransactionId> getSharedLockHolders = new ArrayList<TransactionId>(getSharedLockList(pid));
    	
    	//Add the exclusive lock holder too.
    	if(exclusiveLocksMap.get(pid) != null)
    		getSharedLockHolders.add(exclusiveLocksMap.get(pid));

    	for(TransactionId holder: getSharedLockHolders)
    	{
    		if (tid.equals(holder))
    			continue;
    		if (getAffectedPagesList(tid).contains(intentionLock.get(holder)))
    			throw new TransactionAbortedException();	
    	}
    }
}