package de.uni_mannheim.informatik.wdi.spark;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.spark.api.java.*;

import de.uni_mannheim.informatik.wdi.model.DataSet;
import de.uni_mannheim.informatik.wdi.model.DefaultDataSet;
import de.uni_mannheim.informatik.wdi.model.Matchable;

public class SparkDataSet<RecordType extends Matchable, SchemaElementType> extends DefaultDataSet<RecordType, SchemaElementType> implements SparkCollection<RecordType> {
	
	//TODO add features to load RDDs directly using Spark's load functions
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private JavaRDD<RecordType> distributedData = null;
	private transient JavaSparkContext sparkContext = null;
	
	public SparkDataSet() {
		
	}
	private DataSet<RecordType, SchemaElementType> actualDataset = null;
	
	public SparkDataSet(JavaSparkContext sc) {
		super();
		sparkContext = sc;
		actualDataset = this;
	}
	
	public SparkDataSet(JavaSparkContext sc, DataSet<RecordType, SchemaElementType> dataset) {
		super();
		sparkContext = sc;
		actualDataset = dataset;
	}
	
	public void parallelize() {
		if(!isDistributed()) {
			distributedData = sparkContext.parallelize(new ArrayList<>(actualDataset.getRecords()));
		}
	}

	public boolean isDistributed() {
		return distributedData != null;
	}
	
	public JavaRDD<RecordType> getDistributedData() {
		return distributedData;
	}
	
	public void setDistributedData(JavaRDD<RecordType> data) {
		this.distributedData = data;
	}
	
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.model.DefaultDataSet#get()
	 */
	@Override
	public Collection<RecordType> get() {
		return distributedData.collect();
	}
	
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.model.DefaultDataSet#getSize()
	 */
	//too slow!
//	@Override
//	public int getSize() {
//		if(isDistributed()) {
//			return (int)distributedData.count();
//		} else {
//			return super.getSize();
//		}
//	}

}
