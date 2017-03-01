package de.uni_mannheim.informatik.wdi.spark.blocking;

import org.apache.spark.api.java.function.PairFunction;

import de.uni_mannheim.informatik.wdi.matching.blocking.BlockingKeyGenerator;
import de.uni_mannheim.informatik.wdi.model.Matchable;
import scala.Tuple2;

@Deprecated
public class SparkBlockingFunction<RecordType extends Matchable> implements PairFunction<RecordType, String, RecordType> {
	private static final long serialVersionUID = 1L;

	private BlockingKeyGenerator<RecordType> blockingKeyGenerator;
	
	public BlockingKeyGenerator<RecordType> getBlockingKeyGenerator() {
		return blockingKeyGenerator;
	}
	public void setBlockingKeyGenerator(
			BlockingKeyGenerator<RecordType> blockingKeyGenerator) {
		this.blockingKeyGenerator = blockingKeyGenerator;
	}
	
	public SparkBlockingFunction() {
	}
	
	public SparkBlockingFunction(BlockingKeyGenerator<RecordType> blockingKeyGenerator) {
		setBlockingKeyGenerator(blockingKeyGenerator);
	}
	
	@Override
	public Tuple2<String, RecordType> call(RecordType record) throws Exception {
		return new Tuple2<String, RecordType>(blockingKeyGenerator.getBlockingKey(record), record);
	}
}