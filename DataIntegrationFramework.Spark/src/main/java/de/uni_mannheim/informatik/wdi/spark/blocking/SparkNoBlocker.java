package de.uni_mannheim.informatik.wdi.spark.blocking;

import de.uni_mannheim.informatik.wdi.matching.blocking.StaticBlockingKeyGenerator;
import de.uni_mannheim.informatik.wdi.model.Matchable;

@Deprecated
public class SparkNoBlocker<RecordType extends Matchable, SchemaElementType extends Matchable> extends SparkStandardBlocker<RecordType, SchemaElementType> {

	public SparkNoBlocker() {
		super(new StaticBlockingKeyGenerator<RecordType>());
	}

}
