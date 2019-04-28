package simpledb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The Catalog keeps track of all available tables in the database and their
 * associated schemas.
 * For now, this is a stub catalog that must be populated with tables by a
 * user program before it can be used -- eventually, this should be converted
 * to a catalog that reads a catalog table from disk.
 * 
 * @Threadsafe
 */
public class Catalog {

	private java.util.Map<java.lang.Integer,java.lang.String> id2name;
	private java.util.Map<java.lang.Integer,DbFile>	id2table;
	private java.util.Map<java.lang.Integer,TupleDesc> id2tupledesc;
	private java.util.Map<java.lang.String,java.lang.Integer> name2id;
	private java.util.Map<java.lang.Integer,java.lang.String> pkey;
    
	/***
     * Constructor.
     * Creates a new, empty catalog.
     */
    public Catalog() {
    	
        this.id2name = new HashMap<>();
        this.id2table = new HashMap<>();
        this.id2tupledesc = new HashMap<>();
        this.name2id = new HashMap<>();
        this.pkey = new HashMap<>();
    }

    /**
     * Add a new table to the catalog.
     * This table's contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     * @param name the name of the table -- may be an empty string.  May not be null.  If a name
     * conflict exists, use the last table to be added as the table for a given name.
     * @param pkeyField the name of the primary key field
     */
    public void addTable(DbFile file, String name, String pkeyField) {
    	
    	this.id2name.put(file.getId(), name);
    	this.id2table.put(file.getId(), file);
    	this.id2tupledesc.put(file.getId(), file.getTupleDesc());
    	this.name2id.put(name, file.getId());
    	this.pkey.put(file.getId(), pkeyField);
    }

    public void addTable(DbFile file, String name) {
        addTable(file, name, "");
    }

    /**
     * Add a new table to the catalog.
     * This table has tuples formatted using the specified TupleDesc and its
     * contents are stored in the specified DbFile.
     * @param file the contents of the table to add;  file.getId() is the identfier of
     *    this file/tupledesc param for the calls getTupleDesc and getFile
     */
    public void addTable(DbFile file) {
        addTable(file, (UUID.randomUUID()).toString());
    }

    /**
     * Return the id of the table with a specified name,
     * @throws NoSuchElementException if the table doesn't exist
     */
    public int getTableId(String name) throws NoSuchElementException {
        
    	if(this.name2id.get(name) != null)
    		return this.name2id.get(name);
    	throw new NoSuchElementException("No id found with given table name");
    }

    /**
     * Returns the tuple descriptor (schema) of the specified table
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     * @throws NoSuchElementException if the table doesn't exist
     */
    public TupleDesc getTupleDesc(int tableid) throws NoSuchElementException {
    	
        if(this.id2table.get(tableid) != null)
        	return this.id2table.get(tableid).getTupleDesc();
    	throw new NoSuchElementException("No table found with given table id");
    }

    /**
     * Returns the DbFile that can be used to read the contents of the
     * specified table.
     * @param tableid The id of the table, as specified by the DbFile.getId()
     *     function passed to addTable
     */
    public DbFile getDatabaseFile(int tableid) throws NoSuchElementException {
        
    	if(this.id2table.get(tableid) != null)
        	return this.id2table.get(tableid);
    	throw new NoSuchElementException("No table found with given table id");
    }

    public String getPrimaryKey(int tableid) {
    	if(this.pkey.get(tableid) != null)
        	return this.pkey.get(tableid);
    	
    	throw new NoSuchElementException("No primary key found with given table id");
    }

    public Iterator<Integer> tableIdIterator() {
    	
    	return this.id2table.keySet().iterator();
    }

    public String getTableName(int id) {
        
    	if(this.id2name.get(id) != null)
        	return this.id2name.get(id);
    	throw new NoSuchElementException("No table found with given table id");
    }
    
    /** Delete all tables from the catalog */
    public void clear() {
    	
    	this.id2name.clear();;
        this.id2table.clear();
        this.id2tupledesc.clear();
        this.name2id.clear();
        this.pkey.clear();
    }
    
    /**
     * Reads the schema from a file and creates the appropriate tables in the database.
     * @param catalogFile
     */
    public void loadSchema(String catalogFile) {
        String line = "";
        String baseFolder=new File(new File(catalogFile).getAbsolutePath()).getParent();
        try {
            BufferedReader br = new BufferedReader(new FileReader(new File(catalogFile)));
            
            while ((line = br.readLine()) != null) {
                //assume line is of the format name (field type, field type, ...)
                String name = line.substring(0, line.indexOf("(")).trim();
                //System.out.println("TABLE NAME: " + name);
                String fields = line.substring(line.indexOf("(") + 1, line.indexOf(")")).trim();
                String[] els = fields.split(",");
                ArrayList<String> names = new ArrayList<String>();
                ArrayList<Type> types = new ArrayList<Type>();
                String primaryKey = "";
                for (String e : els) {
                    String[] els2 = e.trim().split(" ");
                    names.add(els2[0].trim());
                    if (els2[1].trim().toLowerCase().equals("int"))
                        types.add(Type.INT_TYPE);
                    else if (els2[1].trim().toLowerCase().equals("string"))
                        types.add(Type.STRING_TYPE);
                    else {
                        System.out.println("Unknown type " + els2[1]);
                        System.exit(0);
                    }
                    if (els2.length == 3) {
                        if (els2[2].trim().equals("pk"))
                            primaryKey = els2[0].trim();
                        else {
                            System.out.println("Unknown annotation " + els2[2]);
                            System.exit(0);
                        }
                    }
                }
                Type[] typeAr = types.toArray(new Type[0]);
                String[] namesAr = names.toArray(new String[0]);
                TupleDesc t = new TupleDesc(typeAr, namesAr);
                HeapFile tabHf = new HeapFile(new File(baseFolder+"/"+name + ".dat"), t);
                addTable(tabHf,name,primaryKey);
                System.out.println("Added table : " + name + " with schema " + t);
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(0);
        } catch (IndexOutOfBoundsException e) {
            System.out.println ("Invalid catalog entry : " + line);
            System.exit(0);
        }
    }
}

