package de.uni_mannheim.informatik.wdi.spark;

import org.apache.spark.api.java.function.Function;

import de.uni_mannheim.informatik.wdi.matching.SchemaMatchingRule;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.Matchable;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.ResultSet;

public class SparkSchemaMatchingWithVotingFunction<RecordType extends Matchable, SchemaElementType extends Matchable> implements Function<Correspondence<RecordType, SchemaElementType>, Pair<Correspondence<RecordType, SchemaElementType>,ResultSet<Correspondence<SchemaElementType, RecordType>>>> {

	private static final long serialVersionUID = 1L;
	private SchemaMatchingRule<RecordType, SchemaElementType, SchemaElementType> rule;
	public void setRule(SchemaMatchingRule<RecordType, SchemaElementType, SchemaElementType> rule) {
		this.rule = rule;
	}
	public SchemaMatchingRule<RecordType, SchemaElementType, SchemaElementType> getRule() {
		return rule;
	}
	
	private SparkDataSet<SchemaElementType, SchemaElementType> schema1;
	private SparkDataSet<SchemaElementType, SchemaElementType> schema2;
	
	public SparkDataSet<SchemaElementType, SchemaElementType> getSchema1() {
		return schema1;
	}
	public SparkDataSet<SchemaElementType, SchemaElementType> getSchema2() {
		return schema2;
	}
	
	public void setSchema1(SparkDataSet<SchemaElementType, SchemaElementType> schema1) {
		this.schema1 = schema1;
	}
	public void setSchema2(SparkDataSet<SchemaElementType, SchemaElementType> schema2) {
		this.schema2 = schema2;
	}
	
	@Override
	public Pair<Correspondence<RecordType, SchemaElementType>, ResultSet<Correspondence<SchemaElementType, RecordType>>> call(
			Correspondence<RecordType, SchemaElementType> arg0) throws Exception {
		return new Pair<>(arg0, rule.apply(schema1, schema2, arg0));
	}

}
