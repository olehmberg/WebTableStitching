package de.uni_mannheim.informatik.wdi.spark.blocking;

import java.util.Collection;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;
import de.uni_mannheim.informatik.wdi.matching.MatchingTask;
import de.uni_mannheim.informatik.wdi.matching.blocking.BlockedMatchable;
import de.uni_mannheim.informatik.wdi.model.Matchable;
import de.uni_mannheim.informatik.wdi.model.ResultSet;

public class SparkBlockingResult<RecordType extends Matchable, SchemaElementType extends Matchable> extends ResultSet<BlockedMatchable<RecordType, SchemaElementType>> {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	JavaPairRDD<String, Tuple2<RecordType, RecordType>> blockedData;
	
	public void setBlockedData(
			JavaPairRDD<String, Tuple2<RecordType, RecordType>> blockedData) {
		this.blockedData = blockedData;
	}
	
	public JavaPairRDD<String, Tuple2<RecordType, RecordType>> getBlockedData() {
		return blockedData;
	}
	
	/**
	 * Turns a spark blocking result into a collection of BlockedMatchable
	 * 
	 * We cannot move this to its own class because the second type parameter has to be the same as the one for the SparkBlockingResult class
	 * @author Oliver
	 *
	 * @param <RecordType>
	 * @param <SchemaElementType>
	 */
	public class SparkBlockedMatchableCollectionFunction implements Function<Tuple2<String,Tuple2<RecordType,RecordType>>, BlockedMatchable<RecordType, SchemaElementType>> {

		private static final long serialVersionUID = 1L;

		@Override
		public BlockedMatchable<RecordType, SchemaElementType> call(
				Tuple2<String, Tuple2<RecordType, RecordType>> arg0)
				throws Exception {
			return new MatchingTask<>(arg0._2()._1(), arg0._2()._2(), null);
		}

	}
	
	@Override
	public Collection<BlockedMatchable<RecordType, SchemaElementType>> get() {
		return blockedData.map(new SparkBlockedMatchableCollectionFunction()).collect();
	}

}
