package de.uni_mannheim.informatik.wdi.spark;

import org.apache.spark.api.java.function.Function;

import de.uni_mannheim.informatik.wdi.model.Correspondence;

class SparkFilterCorrespondenceFunction<RecordType, SchemaElementType> implements Function<Correspondence<RecordType, SchemaElementType>, Boolean> {
	private static final long serialVersionUID = 1L;

	@Override
	public Boolean call(Correspondence<RecordType, SchemaElementType> arg0)
			throws Exception {
		return arg0!=null;
	}
}