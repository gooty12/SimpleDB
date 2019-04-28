package simpledb;

import java.util.HashMap;
import java.util.Vector;

import simpledb.Predicate.Op;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

	HashMap<Integer, Integer> histogramBuckets = new HashMap<Integer, Integer>();
	HashMap<Integer, Integer> histogramBucketsMaxValue = new HashMap<Integer, Integer>();
	HashMap<Integer, Integer> histogramBucketsMinValue = new HashMap<Integer, Integer>();

	int buckets;
	int min;
	int max;
	int widthOfBucket;
	int countOfValues;
	
    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	this.widthOfBucket = ((max - min) + 1)/buckets;
    	if(this.widthOfBucket < 1)
    		this.widthOfBucket = 1;
    	this.buckets = buckets;
    	
    	for(int i = 0 ; i < this.buckets ; i++) {
    		this.histogramBuckets.put(i, 0);
    		this.histogramBucketsMaxValue.put(i, Integer.MIN_VALUE);
    		this.histogramBucketsMinValue.put(i, Integer.MAX_VALUE);
    	}
    		
    	this.min = min;
    	this.max = max;
    	this.countOfValues = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	int diffValue = v - this.min;
    	this.countOfValues++;
    	int bucketPosition = (int) Math.ceil(diffValue/this.widthOfBucket);
    	if(this.histogramBuckets.containsKey(bucketPosition)) {
    		int countValue = this.histogramBuckets.get(bucketPosition) + 1;
    		this.histogramBuckets.put(bucketPosition, countValue);
    		
    		int maxValue = this.histogramBucketsMaxValue.get(bucketPosition);
    		int minValue = this.histogramBucketsMinValue.get(bucketPosition);
    		
    		if(v > maxValue)
    			this.histogramBucketsMaxValue.put(bucketPosition, v);
    		if(v < minValue)
    			this.histogramBucketsMinValue.put(bucketPosition, v);
    	}		
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */public double estimateSelectivity(Predicate.Op op, int v) {
 
    	 if(op == Op.EQUALS)
    		 return estimateSelectivityUtility(Op.EQUALS, v);
    	 if(op == Op.NOT_EQUALS)
    		 return 1 - estimateSelectivityUtility(Op.EQUALS, v);
    	 if(op == Op.GREATER_THAN)
    		 return estimateSelectivityUtility(Op.GREATER_THAN, v);
    	 if(op == Op.GREATER_THAN_OR_EQ)
    		 return estimateSelectivityUtility(Op.GREATER_THAN, v) + estimateSelectivityUtility(Op.EQUALS, v);
    	 if(op == Op.LESS_THAN)
    		 return estimateSelectivityUtility(Op.LESS_THAN, v);
    	 if(op == Op.LESS_THAN_OR_EQ)
    		 return estimateSelectivityUtility(Op.LESS_THAN, v) + estimateSelectivityUtility(Op.EQUALS, v);
    	 return -1.0;
     }
    	    
    public double estimateSelectivityUtility(Predicate.Op op, int v) {
    	
    	if(op == Op.EQUALS) {
    		
    		int diffValue = v - this.min;
        	int bucketPosition = (int) Math.ceil(diffValue/this.widthOfBucket);
        	if(bucketPosition < 0 || bucketPosition > this.buckets - 1)
        		return 0;
        	int countValue = this.histogramBuckets.get(bucketPosition);
        	return (double)countValue/(double)(this.widthOfBucket*this.countOfValues);
    	}
   
    	if(op == Op.GREATER_THAN || op == Op.GREATER_THAN_OR_EQ) {
    		
    		int diffValue = v - this.min;
        	
    		int bucketPosition = (int) Math.ceil(diffValue/this.widthOfBucket);
        	
    		if (bucketPosition > this.buckets - 1)
                return 0;
    		
            if (bucketPosition < 0)
                return 1;
            
    		System.out.println(bucketPosition + " " + diffValue + " " + v + " " + this.min);
    		int countValue = this.histogramBuckets.get(bucketPosition);
        	
        	double bf =  (double)countValue/(double)(this.countOfValues);
        	
        	int maxValue = this.histogramBucketsMaxValue.get(bucketPosition);
        	
        	double bpart = (double)(maxValue - v)/(double)(this.widthOfBucket);
        	
        	double sumOfGreater = bpart * bf;
        	
        	for(int i = bucketPosition + 1 ; i < this.buckets ; i++) {
        		
        		countValue = this.histogramBuckets.get(i);
        		sumOfGreater += (double)countValue/(double)(this.countOfValues);
        	}
        	
        	return sumOfGreater;
    	}
    	
    	if(op == Op.LESS_THAN || op == Op.LESS_THAN_OR_EQ) {
    		
    		int diffValue = v - this.min;
        	
    		
    		int bucketPosition = (int) Math.ceil(diffValue/this.widthOfBucket);
        	
    		if (bucketPosition > this.buckets - 1)
                return 1;
    		
            if (bucketPosition < 0)
                return 0;
            
    		int countValue = this.histogramBuckets.get(bucketPosition);
        	
        	double bf =  (double)countValue/(double)(this.countOfValues);
        	
        	int minValue = this.histogramBucketsMinValue.get(bucketPosition);
        	
        	double bpart = (double)(v - minValue)/(double)(this.widthOfBucket);
        	
        	double sumOfLess = bpart * bf;
        	
        	for(int i = bucketPosition - 1 ; i >= 0 ; i--) {
        		
        		countValue = this.histogramBuckets.get(i);
        		sumOfLess += (double)countValue/(double)(this.countOfValues);
        	}
        	
        	return sumOfLess;
    	}
    	
        return -1.0;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return this.toString();
    }
}
