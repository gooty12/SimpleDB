package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId tid;
    private DbIterator child;
    private boolean isDeleted = false;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
    	this.tid = t;
    	this.child = child;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return Utility.getTupleDesc(1);
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
    	super.open();
    	child.open();
    }

    public void close() {
        // some code goes here
    	super.close();
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
    	child.rewind();
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
    	int count = 0;
        if(!isDeleted){
            while(child.hasNext()){
                Tuple tuple = child.next();
                try{
                    Database.getBufferPool().deleteTuple(tid, tuple);
                } catch (IOException e) {
                    throw new DbException("Not able to delete tuple");
                }
                count++;
            }
            isDeleted = true;
            return Utility.getTuple(new int[]{count}, 1);
        }
        return null;
        
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
    	return new DbIterator[]{child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
    	if(children.length > 0){
            this.child = children[0];
        }
    }

}
