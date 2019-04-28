package simpledb;

import java.util.ArrayList;
import java.util.Iterator;

import simpledb.Aggregator.Op;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private ArrayList<Tuple> aggregatedTuples = new ArrayList<Tuple>();

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
    	TupleDesc aggregatedDesc;
    	
		int groupingFlag;
		int outputAggregatedField;
		int outputGroupByField;
    	
		if(gbfield != Aggregator.NO_GROUPING) {
    		
    		aggregatedDesc = new TupleDesc(new Type[] {gbfieldtype, Type.INT_TYPE});
    		groupingFlag = 1;
    		
    		outputAggregatedField = 1;
    		outputGroupByField = 0;
    	} else {
    		
    		aggregatedDesc = new TupleDesc(new Type[] {Type.INT_TYPE});
    		groupingFlag = 0;
    		
    		outputAggregatedField = 0;
    		outputGroupByField = -1;
    	}
		
    	Iterator<Tuple> it = aggregatedTuples.iterator();
    	
    	if(!aggregatedTuples.isEmpty()) {
    		
    		while (it.hasNext()) {
                Tuple t = it.next();
                
                Field f1 = t.getField(outputGroupByField);
                Field f2 = tup.getField(this.gbfield);
                
                if(groupingFlag == 0) {
                    int existingValue = ( (IntField) t.getField(outputAggregatedField)).getValue();
                    IntField newField = new IntField(existingValue + 1);
                    t.setField(outputAggregatedField, newField);
                    return;
                }

               
                if (f1.equals(f2)) {
                    int existingValue = ( (IntField) t.getField(outputAggregatedField)).getValue();
                    IntField newField = new IntField(existingValue + 1);
                    t.setField(outputAggregatedField, newField);
                    return;
                }
            }
    	}
        		
        Tuple aggregatedTuple = new Tuple(aggregatedDesc);

        Field initialField = new IntField(1);
            
        if(this.what != Op.COUNT)
        	initialField = tup.getField(afield);
        	
        if(groupingFlag == 0) {
        	
        	aggregatedTuple.setField(outputAggregatedField, initialField);
        } else {
        	
        	Field grpField = tup.getField(this.gbfield);
        	
        	aggregatedTuple.setField(outputGroupByField, grpField);        	
        	aggregatedTuple.setField(outputAggregatedField, initialField);
        }
        	
        aggregatedTuples.add(aggregatedTuple);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
    	
    	TupleDesc aggregatedDesc;
    	if(gbfield != Aggregator.NO_GROUPING) {
    		
    		aggregatedDesc = new TupleDesc(new Type[] {gbfieldtype, Type.INT_TYPE});
    	} else {
    		
    		aggregatedDesc = new TupleDesc(new Type[] {Type.INT_TYPE});
    	}
        return new TupleIterator(aggregatedDesc, aggregatedTuples);
    }

}
