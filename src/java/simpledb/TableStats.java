package simpledb;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import com.sun.org.apache.bcel.internal.generic.NEW;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1, lab2 and lab3.
 */
public class TableStats {

    private static final ConcurrentHashMap<String, TableStats> statsMap = new ConcurrentHashMap<String, TableStats>();

    private int basePages;
    private int baseTups;
    private int tableId;
    Vector<Object> histograms = new Vector<Object>();
    Vector<Integer> minValues = new Vector<Integer>();
    Vector<Integer> maxValues = new Vector<Integer>();
    static final int IOCOSTPERPAGE = 1000;
    private TupleDesc baseTupleDesc;

	private int ioCostPerPage;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(HashMap<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        } catch (SecurityException e) {
            e.printStackTrace();
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
    	DbFile dbfile = Database.getCatalog().getDatabaseFile(tableid);
    	baseTupleDesc = dbfile.getTupleDesc();
    	this.tableId = tableId;
    	this.ioCostPerPage = ioCostPerPage;
    	
    	this.basePages = ((HeapFile)dbfile).numPages();
    	
    	TransactionId tempId = new TransactionId();
    	SeqScan s = new SeqScan(tempId, tableid);
    	this.baseTups = 0;
    	for(int i = 0 ; i < baseTupleDesc.numFields(); i++) {
    		this.maxValues.add(Integer.MIN_VALUE);
    		this.minValues.add(Integer.MAX_VALUE);
    	}
    	try {
    		
			s.open();
			while(s.hasNext()) {
				Tuple current = s.next();
				this.baseTups++;
				for(int i = 0 ; i < baseTupleDesc.numFields() ; i++) {
					if(current.getField(i).getType() == Type.INT_TYPE) {
						if(((IntField)current.getField(i)).getValue() > this.maxValues.get(i)) {
							
							this.maxValues.add(i, ((IntField)current.getField(i)).getValue());
						}
						
						if(((IntField)current.getField(i)).getValue() < this.minValues.get(i)) {
							
							this.minValues.add(i, ((IntField)current.getField(i)).getValue());
						}
							
					}
				}
			}
			
			for(int i = 0 ; i < baseTupleDesc.numFields(); i++) {
				if(baseTupleDesc.getFieldType(i) == Type.INT_TYPE)
					this.histograms.add(new IntHistogram(NUM_HIST_BINS, this.minValues.get(i), this.maxValues.get(i)));
				else
					this.histograms.add(new StringHistogram(NUM_HIST_BINS));
			}
			
			s.rewind();
			
			while(s.hasNext()) {
				Tuple current = s.next();
				
				for(int i = 0 ; i < baseTupleDesc.numFields() ; i++) {
					if(baseTupleDesc.getFieldType(i) == Type.INT_TYPE)
						((IntHistogram)this.histograms.get(i)).addValue(((IntField)current.getField(i)).getValue());
					else
						((StringHistogram)this.histograms.get(i)).addValue(((StringField)current.getField(i)).getValue());
	
				}
			}
			s.close();
		} catch (DbException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (TransactionAbortedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	
    	
    	
    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        
        return this.basePages*this.ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int) (this.baseTups*selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        if(constant.getType() == Type.INT_TYPE)
        	return ((IntHistogram)this.histograms.get(field)).estimateSelectivity(op, ((IntField)constant).getValue());
        else
        	return ((StringHistogram)this.histograms.get(field)).estimateSelectivity(op, ((StringField)constant).getValue());
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return this.baseTups;
    }

}
