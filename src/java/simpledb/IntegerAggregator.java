package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private ArrayList<Tuple> aggregatedTuples = new ArrayList<Tuple>();
    private HashMap<Field, Integer> countOfFields = new HashMap<Field, Integer>();
    private HashMap<Field, Integer> sumOfFields = new HashMap<Field, Integer>();
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        
    	this.gbfield = gbfield;
    	this.gbfieldtype = gbfieldtype;
    	this.afield = afield;
    	this.what = what;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
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
                if(groupingFlag == 0) {
                    IntField newField = new IntField(combineValue(t, tup, outputAggregatedField));
                    t.setField(outputAggregatedField, newField);
                    return;
                }

                Field f1 = t.getField(outputGroupByField);
                Field f2 = tup.getField(this.gbfield);
                if (f1.equals(f2)) {
                    IntField newField = new IntField(combineValue(t, tup, outputAggregatedField));
                    t.setField(outputAggregatedField, newField);
                    return;
                }
            }
    	}
        		
        Tuple aggregatedTuple = new Tuple(aggregatedDesc);

        Field initialField = new IntField(1);;
        Field grpField = new IntField(-1);
            
        if(this.what != Op.COUNT)
        	initialField = tup.getField(afield);
        	
        if(groupingFlag == 0) {
        	
        	aggregatedTuple.setField(outputAggregatedField, initialField);
        } else {
        	
        	grpField = tup.getField(this.gbfield);
        	
        	aggregatedTuple.setField(outputGroupByField, grpField);        	
        	aggregatedTuple.setField(outputAggregatedField, initialField);
        }
        	
        aggregatedTuples.add(aggregatedTuple);
        
        sumOfFields.put(grpField,((IntField)tup.getField(afield)).getValue());
        countOfFields.put(grpField, 1);
    }

    private int combineValue(Tuple t, Tuple tup, int outputAggregatedField){
    	
        int presentValue = ( (IntField) t.getField(outputAggregatedField)).getValue();
        int currentValue = ((IntField) tup.getField(this.afield)).getValue();
        
        if(this.what == Op.COUNT)
        	
        	return presentValue + 1;
        
        else if(this.what == Op.MIN) {
        	if(presentValue < currentValue)
        		return presentValue;
        	return currentValue;
        }
        
        else if(this.what == Op.MAX){
        	if(presentValue > currentValue)
        		return presentValue;
        	return currentValue;
        }
        
        else if(this.what == Op.SUM)
        	return presentValue + currentValue;
        else if(this.what == Op.AVG) {
        	
        	Field tupleKey;
        	
            if (this.gbfield != Aggregator.NO_GROUPING)
            	tupleKey = tup.getField(this.gbfield);
            else
            	tupleKey = new IntField(-1);
                
            int curCount = countOfFields.get(tupleKey) + 1;
            int curSum = sumOfFields.get(tupleKey) + currentValue;
                
            countOfFields.put(tupleKey, curCount);
            sumOfFields.put(tupleKey, curSum);
            
            return curSum/curCount;
        }
        return 0;
    }
    
    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
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
