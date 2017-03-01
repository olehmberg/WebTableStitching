package de.uni_mannheim.informatik.wdi.spark;

import org.apache.spark.api.java.function.Function;

import de.uni_mannheim.informatik.wdi.matching.MatchingRule;
import de.uni_mannheim.informatik.wdi.matching.blocking.BlockedMatchable;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.Matchable;

class SparkRecordMatchingFunction<RecordType extends Matchable, SchemaElementType> implements Function<BlockedMatchable<RecordType,SchemaElementType>, Correspondence<RecordType, SchemaElementType>> {
	
	private static final long serialVersionUID = 1L;

	protected SparkResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences;
	public void setSchemaCorrespondences(
			SparkResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences) {
		this.schemaCorrespondences = schemaCorrespondences;
	}
	public SparkResultSet<Correspondence<SchemaElementType, RecordType>> getSchemaCorrespondences() {
		return schemaCorrespondences;
	}
	
	protected MatchingRule<RecordType, SchemaElementType> rule;
	
	public MatchingRule<RecordType, SchemaElementType> getRule() {
		return rule;
	}
	public void setRule(MatchingRule<RecordType, SchemaElementType> rule) {
		this.rule = rule;
	}
	
	public SparkRecordMatchingFunction() {
	}
	
	public SparkRecordMatchingFunction(MatchingRule<RecordType, SchemaElementType> rule) {
		setRule(rule);
	}
	
//	@Override
//	public Correspondence<RecordType, SchemaElementType> call(
//			Tuple2<String, Tuple2<RecordType, RecordType>> arg0)
//			throws Exception {
//		// apply the Duplicate Detection rule
//		Correspondence<RecordType, SchemaElementType> cor = getRule().apply(arg0._2()._1(),
//				arg0._2()._2(), getSchemaCorrespondences());
//
//		return cor;
//	}
	
	@Override
	public Correspondence<RecordType, SchemaElementType> call(
			BlockedMatchable<RecordType, SchemaElementType> v1)
			throws Exception {
		
		Correspondence<RecordType, SchemaElementType> cor = getRule().apply(v1.getFirstRecord(), v1.getSecondRecord(), v1.getSchemaCorrespondences());
		return cor;
	}
	
}