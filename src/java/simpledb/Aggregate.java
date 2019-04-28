package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private DbIterator child;
    private int afield;
    private int gfield;
    private Aggregator.Op aop;
    private Aggregator typeOfAggregator;
    private DbIterator aggregationIterator;
    private TupleDesc inputDescriptor, outputDescriptor;
    
    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
		this.child = child;
		this.afield = afield;
		this.gfield = gfield;
		this.aop = aop;
		setMetaValues();
    }

    private void setMetaValues() {
		// TODO Auto-generated method stub
		
        Type groupingFieldType;
        Type[] types;
        String[] names;
        this.inputDescriptor = this.child.getTupleDesc();

        String aggregatedFieldName =  aop.toString() + "(" + inputDescriptor.getFieldName(afield) + ")";
        //String aggregatedFieldName =  inputDescriptor.getFieldName(afield);
    	
    	if(gfield == Aggregator.NO_GROUPING) {
            
    		groupingFieldType = null;
            types = new Type[] {Type.INT_TYPE};
            names = new String[] {aggregatedFieldName};
        } else {
        	
            groupingFieldType = inputDescriptor.getFieldType(this.gfield);
            types = new Type[]{groupingFieldType, Type.INT_TYPE};
            names = new String[] {inputDescriptor.getFieldName(this.gfield),aggregatedFieldName};
        }
        
        outputDescriptor = new TupleDesc(types, names);
        
        if (inputDescriptor.getFieldType(this.afield) == Type.INT_TYPE)
            typeOfAggregator = new IntegerAggregator(gfield, groupingFieldType, afield, this.aggregateOp());
        else
        	typeOfAggregator = new StringAggregator(gfield, groupingFieldType, afield, this.aggregateOp());
        
        aggregationIterator = typeOfAggregator.iterator();
    }

	/**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
		return this.gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
    	if(this.gfield == Aggregator.NO_GROUPING)
    		return null;
    	return outputDescriptor.getFieldName(0);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
    	return this.afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
		return this.inputDescriptor.getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
		return this.aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
    	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
		super.open();
		openDbIterator();
		aggregationIterator.open();
    }

    private void openDbIterator() throws DbException, TransactionAbortedException {
		// TODO Auto-generated method stub

		this.child.open();
		while(this.child.hasNext()) 
			this.typeOfAggregator.mergeTupleIntoGroup(this.child.next());
		this.child.close();
	}

	/**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
		if(aggregationIterator.hasNext())
			return aggregationIterator.next();
	
		return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
		aggregationIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
    	return this.outputDescriptor;
    }

    public void close() {
		this.aggregationIterator.close();
		super.close();
    }

    @Override
    public DbIterator[] getChildren() {
    	return new DbIterator[] { aggregationIterator };
    }

    @Override
    public void setChildren(DbIterator[] children) {
    	aggregationIterator = children[0];
    }
    
}
