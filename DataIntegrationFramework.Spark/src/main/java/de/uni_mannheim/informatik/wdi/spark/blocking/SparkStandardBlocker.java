package de.uni_mannheim.informatik.wdi.spark.blocking;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import de.uni_mannheim.informatik.wdi.matching.blocking.BlockingKeyGenerator;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.Matchable;
import de.uni_mannheim.informatik.wdi.spark.SparkDataSet;
import de.uni_mannheim.informatik.wdi.spark.SparkResultSet;

@Deprecated
public class SparkStandardBlocker<RecordType extends Matchable, SchemaElementType extends Matchable> extends SparkBlocker<RecordType, SchemaElementType> {

	private BlockingKeyGenerator<RecordType> blockingKeyGenerator;

	public SparkStandardBlocker(BlockingKeyGenerator<RecordType> blockingKeyGenerator) {
		this.blockingKeyGenerator = blockingKeyGenerator;
	}

	@Override
	public JavaPairRDD<String, Tuple2<RecordType, RecordType>> generatePairs(
			SparkDataSet<RecordType, SchemaElementType> dataset1,
			SparkDataSet<RecordType, SchemaElementType> dataset2,
			SparkResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences) {
		
		PairFunction<RecordType, String, RecordType> blockingFunction = new SparkBlockingFunction<>(blockingKeyGenerator);
		
		JavaPairRDD<String, RecordType> pairs1 = dataset1.getDistributedData().mapToPair(blockingFunction);
		JavaPairRDD<String, RecordType> pairs2 = dataset2.getDistributedData().mapToPair(blockingFunction);
		
		JavaPairRDD<String, Tuple2<RecordType, RecordType>> blocked = pairs1.join(pairs2).distinct(); 
		
		return blocked;
	}

	@Override
	public JavaPairRDD<String, Tuple2<RecordType, RecordType>> generatePairs(
			SparkDataSet<RecordType, SchemaElementType> dataset,
			boolean isSymmetric,
			SparkResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences) {
		PairFunction<RecordType, String, RecordType> blockingFunction = new SparkBlockingFunction<>(blockingKeyGenerator);
		
		JavaPairRDD<String, RecordType> pairs = dataset.getDistributedData().mapToPair(blockingFunction);
		
		return pairs.join(pairs).distinct();
	}


}
