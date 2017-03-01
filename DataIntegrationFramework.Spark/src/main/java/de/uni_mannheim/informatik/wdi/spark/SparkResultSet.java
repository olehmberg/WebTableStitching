package de.uni_mannheim.informatik.wdi.spark;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import de.uni_mannheim.informatik.wdi.model.BasicCollection;
import de.uni_mannheim.informatik.wdi.model.ResultSet;

/**
 * Distributed ResultSet. Elements can be added using the add() method, but will only be available after calling parallelize().
 * @author Oliver
 *
 * @param <ElementType>
 */
public class SparkResultSet<ElementType> extends ResultSet<ElementType> implements SparkCollection<ElementType> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private JavaRDD<ElementType> data;
	
	public void setData(JavaRDD<ElementType> data) {
		this.data = data;
	}
	
	@Override
	public JavaRDD<ElementType> getDistributedData() {
		return data;
	}
	
	@Override
	public Collection<ElementType> get() {
		return data.collect();
	}
	
//	@Override
//	public int size() {
//		return (int)data.count();
//	}
	
	public void parallelize(JavaSparkContext sc) {
		data = sc.parallelize(new ArrayList<>(this.elements));
	}
	
	public static <RecordType> SparkResultSet<RecordType> parallelize(BasicCollection<RecordType> dataset, JavaSparkContext sc) {
		SparkResultSet<RecordType> distributed = new SparkResultSet<>();
		distributed.setData(sc.parallelize(new ArrayList<>(dataset.get())));
		return distributed;
	}
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.spark.SparkCollection#parallelize()
	 */
	@Override
	public void parallelize() {
		// TODO Auto-generated method stub
		
	}
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.spark.SparkCollection#isDistributed()
	 */
	@Override
	public boolean isDistributed() {
		// TODO Auto-generated method stub
		return false;
	}

	
	//TODO implement remove()
}
