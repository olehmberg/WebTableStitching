package de.uni_mannheim.informatik.wdi.model;

import java.util.Collection;
import java.util.List;

//TODO make DataSet and ResultSet the only type parameters in all classes in this library?
// a DataSet should have a function to create a new (properly typed) DataSet and/or ResultSet
// use interface Matchable or Object internally to deal with data, externally, only DataSet and ResultSet should be used
// the user of the library defines the type of DataSet, which itself contains type parameters for RecordType and SchemaElementType
// how will this make sure that RecordType of DataSet and MatchingRule (for example) are the same/compatible? - by using the dataset set as type for the MatchingRule?


public interface DataSet<RecordType extends Matchable, SchemaElementType> extends BasicCollection<RecordType> {

	/**
	 * Returns a collection with all entries of this data set.
	 * 
	 * @return
	 */
	public Collection<RecordType> getRecords();

	/**
	 * Returns the entry with the specified identifier or null, if it is not
	 * found.
	 * 
	 * @param identifier
	 *            The identifier of the entry that should be returned
	 * @return
	 */
	public RecordType getRecord(String identifier);

	/**
	 * Returns the number of entries in this data set
	 * 
	 * @return
	 */
	public int getSize();

	/**
	 * Adds an entry to this data set. Any existing entry with the same
	 * identifier will be replaced.
	 * 
	 * @param entry
	 */
	public void addRecord(RecordType record);

	/**
	 * Returns a random record from the data set
	 * 
	 * @return
	 */
	public RecordType getRandomRecord();

	/***
	 * Removes all records from this dataset
	 */
	public void ClearRecords();
	
	public void addAttribute(SchemaElementType attribute);
	public List<SchemaElementType> getAttributes();
	
	public void removeRecord(String identifier);
}