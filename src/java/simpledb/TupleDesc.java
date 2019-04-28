package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    private TupleDesc.TDItem[] tdItems;
    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
    	
    	List<TDItem> l = Arrays.asList(tdItems);
        return l.iterator();
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        
    	this.tdItems = new TupleDesc.TDItem[typeAr.length];
    	for(int i = 0 ; i < typeAr.length ; i++) {
    		
    		this.tdItems[i] = new TupleDesc.TDItem(typeAr[i], fieldAr[i]);
    	}
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {

    	this.tdItems = new TupleDesc.TDItem[typeAr.length];
    	for(int i = 0 ; i < typeAr.length ; i++) {
    		
    		this.tdItems[i] = new TupleDesc.TDItem(typeAr[i], null);
    	}
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        
    	return this.tdItems.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        
    	if(this.tdItems[i].fieldName != null)
    		return this.tdItems[i].fieldName;
    	else 
    		return "null";
    	
    	//throw new NoSuchElementException("No field name found for index");
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        
    	if(this.tdItems[i].fieldType != null)
    		return this.tdItems[i].fieldType;
    	
    	throw new NoSuchElementException("No field type found for index");
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        for(int i = 0 ; i < this.tdItems.length; i++) {
    		if(this.getFieldName(i) != null) {
    			if(this.getFieldName(i).equals(name))
        			return i;
    		}
    	}

    	throw new NoSuchElementException("No index found matching the field name"); 
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
    	int totalSize = 0;
        for(int i = 0 ; i < this.tdItems.length; i++) {
        	
        	totalSize += this.tdItems[i].fieldType.getLen();
        }
        
        return totalSize;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
    	
    	Type[] typeAr = new Type[td1.numFields() + td2.numFields()];
    	String[] fieldAr = new String[td1.numFields() + td2.numFields()];
    	
    	for(int i = 0 ; i < td1.tdItems.length; i++) {
    		typeAr[i] = td1.tdItems[i].fieldType;
    		fieldAr[i] = td1.tdItems[i].fieldName;
    	}
    	
    	for(int i = 0 ; i < td2.tdItems.length; i++) {
    		typeAr[td1.numFields() + i] = td2.tdItems[i].fieldType;
    		fieldAr[td1.numFields() + i] = td2.tdItems[i].fieldName;
    	}
    	
    	return new TupleDesc(typeAr, fieldAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    public boolean equals(Object o) {
        if(!(o instanceof TupleDesc))
        	return false;
        
        TupleDesc typeCastedObject = (TupleDesc) o;
        
        if(this.hashCode() != typeCastedObject.hashCode())
        	return false;
        
        for(int i = 0 ; i < this.tdItems.length ; i++) {
        	
        	//if(tdItems[i].fieldName != typeCastedObject.tdItems[i].fieldName) {
        	//	return false;
        	//}
        	
        	if(tdItems[i].fieldType != typeCastedObject.tdItems[i].fieldType) {
        		return false;
        	}
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
    	
    	//To be improved upon.
    	return this.numFields();
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        StringBuffer sb = new StringBuffer("");
        
        for(int i = 0 ; i < this.tdItems.length; i++) {
        	sb.append(tdItems[i].toString());
        	if(i != this.tdItems.length - 1)
        		sb.append(",");
        }
        return sb.toString();
    }
}
