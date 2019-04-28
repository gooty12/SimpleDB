package simpledb;

import java.io.*;
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
	private int tableId;
	private TupleDesc td;
	
	private class HeapFileIterator implements DbFileIterator {

		private TransactionId tid;
		private Iterator<Tuple> it;
		private int currPageNo;
		
		public HeapFileIterator(TransactionId tid) {
			this.tid = tid;
			currPageNo = -1;
			it = null;
		}
		
		@Override
		public void open() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			currPageNo = 0;
			HeapPageId heapPageID = new HeapPageId(getId(), currPageNo);
			HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, heapPageID, Permissions.READ_ONLY);
			it = heapPage.iterator();
		}

		@Override
		public boolean hasNext() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			if(currPageNo == -1)
				return false;
			
			if(it.hasNext())
				return true;
			
			if(currPageNo + 1 < numPages()) {
				currPageNo++;
				HeapPageId heapPageID = new HeapPageId(getId(), currPageNo);
				HeapPage heapPage = (HeapPage) Database.getBufferPool().getPage(tid, heapPageID, Permissions.READ_ONLY);
				it = heapPage.iterator();
				return it.hasNext();
			}
			
			return false;
		}

		@Override
		public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
			// TODO Auto-generated method stub
			if(hasNext())
				return it.next();
			
			throw new NoSuchElementException();
		}

		@Override
		public void rewind() throws DbException, TransactionAbortedException {
			// TODO Auto-generated method stub
			close();
			open();
		}

		@Override
		public void close() {
			// TODO Auto-generated method stub
			currPageNo= -1;
			it = null;
		}
		
	}
    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     * @throws FileNotFoundException 
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
    	this.f = f;
    	this.td = td;
    	this.tableId = getId();
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
     * you will need to generate this tableid somewhere ensure that each
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
    	
    	try {
    		int pageSize = Database.getBufferPool().getPageSize();
    		int offset = pid.pageNumber() * pageSize;
			RandomAccessFile randomAccessFile = new RandomAccessFile(f, "r");
			randomAccessFile.seek(offset);
			byte[] byteData = HeapPage.createEmptyPageData();
			
			randomAccessFile.read(byteData);
			randomAccessFile.close();
			
			return new HeapPage((HeapPageId) pid, byteData);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
    	 
    	PageId pageId = page.getId();
    	 
        int pageNumber = pageId.pageNumber();
        
        int pageSize = Database.getBufferPool().getPageSize();

        RandomAccessFile dbFile = new RandomAccessFile(this.f, "rw");
        dbFile.skipBytes(pageNumber * pageSize);
        byte[] pageData = page.getPageData();
        dbFile.write(pageData);
        dbFile.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
    	int fileLength = (int)f.length();
    	int pageSize = Database.getBufferPool().getPageSize();
    	return fileLength/pageSize;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
    	
        ArrayList pagesChanged = new ArrayList();
        int i = 0;
        HeapPageId pid;
        HeapPage page = null;
                
        while(i < numPages()) {
        	pid = new HeapPageId(this.getId(), i);
            page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);

        	if(page.getNumEmptySlots() > 0){
        		break;
        	}
        	i++;
        }
        //
        if(i < numPages()) {
            try {
                page.insertTuple(t);
                pagesChanged.add(page);
                return pagesChanged;
            } catch (Exception e){
                throw new DbException("Tuple can not be inserted");
            }
        } else {
        	HeapPageId newPageId = new HeapPageId(this.getId(), numPages());
            HeapPage newPage = new HeapPage(newPageId, HeapPage.createEmptyPageData());
            
            newPage.insertTuple(t);
            
            this.writePage(newPage);
            
            pagesChanged.add(newPage);
            return pagesChanged;
        } 
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
    	
    	RecordId recordId = t.getRecordId();
        PageId  pageId = recordId.getPageId();
                
        if(getId() != pageId.getTableId())
            throw new DbException("record table id does not match with file id");

        ArrayList<Page> pagesChanged = new ArrayList();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
            
        page.deleteTuple(t);
        pagesChanged.add(page);
        
        return pagesChanged;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
    	return new HeapFileIterator(tid);
    }

}

