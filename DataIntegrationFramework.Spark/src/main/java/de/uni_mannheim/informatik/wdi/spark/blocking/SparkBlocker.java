package de.uni_mannheim.informatik.wdi.spark.blocking;


import org.apache.spark.api.java.JavaPairRDD;

import scala.Tuple2;
import de.uni_mannheim.informatik.wdi.matching.blocking.BlockedMatchable;
import de.uni_mannheim.informatik.wdi.matching.blocking.DatasetLevelBlocker;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.DataSet;
import de.uni_mannheim.informatik.wdi.model.Matchable;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.spark.SparkDataSet;
import de.uni_mannheim.informatik.wdi.spark.SparkMatchingEngine;
import de.uni_mannheim.informatik.wdi.spark.SparkResultSet;

@Deprecated
public abstract class SparkBlocker<RecordType extends Matchable, SchemaElementType extends Matchable> extends
		DatasetLevelBlocker<RecordType, SchemaElementType> {

	// actual blocking function
	public abstract JavaPairRDD<String, Tuple2<RecordType, RecordType>> generatePairs(
			SparkDataSet<RecordType, SchemaElementType> dataset1, SparkDataSet<RecordType, SchemaElementType> dataset2,
			SparkResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences);

	public abstract JavaPairRDD<String, Tuple2<RecordType, RecordType>> generatePairs( 
			SparkDataSet<RecordType, SchemaElementType> dataset, boolean isSymmetric,
			SparkResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences);
	
	// spark-specific interface
	
	// methods from DatasetLevelBlocker 
	public SparkBlockingResult<RecordType, SchemaElementType> block(
			SparkDataSet<RecordType, SchemaElementType> dataset1, SparkDataSet<RecordType, SchemaElementType> dataset2,
			SparkResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences) {
		SparkBlockingResult<RecordType, SchemaElementType> result = new SparkBlockingResult<>();
		result.setBlockedData(generatePairs(dataset1, dataset2, schemaCorrespondences));
		return result;
	}

	public SparkBlockingResult<RecordType, SchemaElementType> block(
			SparkDataSet<RecordType, SchemaElementType> dataset, boolean isSymmetric,
			SparkResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences) {
		SparkBlockingResult<RecordType, SchemaElementType> result = new SparkBlockingResult<>();
		result.setBlockedData(generatePairs(dataset, isSymmetric, schemaCorrespondences));
		return result;
	}
	
	// methods from Blocker
	public SparkBlockingResult<RecordType, SchemaElementType> runBlocking(
			SparkDataSet<RecordType, SchemaElementType> dataset1, SparkDataSet<RecordType, SchemaElementType> dataset2,
			SparkResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences,
			SparkMatchingEngine<RecordType, SchemaElementType> engine) {
		return block(dataset1, dataset2, schemaCorrespondences);
	}
	
	public SparkBlockingResult<RecordType, SchemaElementType> runBlocking(
			SparkDataSet<RecordType, SchemaElementType> dataset, boolean isSymmetric,
			SparkResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences,
			SparkMatchingEngine<RecordType, SchemaElementType> engine) {
		return block(dataset, isSymmetric, schemaCorrespondences);
	}
	
	// DatasetLevelBlocker implementation
	
	@Override
	public ResultSet<BlockedMatchable<RecordType, SchemaElementType>> block(
			DataSet<RecordType, SchemaElementType> dataset1, DataSet<RecordType, SchemaElementType> dataset2,
			ResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences) {
		return block((SparkDataSet<RecordType, SchemaElementType>)dataset1, (SparkDataSet<RecordType, SchemaElementType>)dataset2, (SparkResultSet<Correspondence<SchemaElementType, RecordType>>)schemaCorrespondences);
	}
	
	@Override
	public ResultSet<BlockedMatchable<RecordType, SchemaElementType>> block(
			DataSet<RecordType, SchemaElementType> dataset, boolean isSymmetric,
			ResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences) {
		return block((SparkDataSet<RecordType, SchemaElementType>)dataset, isSymmetric, (SparkResultSet<Correspondence<SchemaElementType, RecordType>>)schemaCorrespondences);
	}

}
