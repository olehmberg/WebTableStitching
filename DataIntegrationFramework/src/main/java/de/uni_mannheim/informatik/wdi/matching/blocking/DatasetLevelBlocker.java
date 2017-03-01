package de.uni_mannheim.informatik.wdi.matching.blocking;

import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.DataSet;
import de.uni_mannheim.informatik.wdi.model.DefaultDataSet;
import de.uni_mannheim.informatik.wdi.model.Matchable;
import de.uni_mannheim.informatik.wdi.model.Record;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;
import de.uni_mannheim.informatik.wdi.similarity.SimilarityMeasure;

public abstract class DatasetLevelBlocker<RecordType extends Matchable, SchemaElementType extends Matchable> extends Blocker<RecordType, SchemaElementType> {

	@Override
	public ResultSet<BlockedMatchable<RecordType, SchemaElementType>> runBlocking(
			DataSet<RecordType, SchemaElementType> dataset1, DataSet<RecordType, SchemaElementType> dataset2,
			ResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences,
			DataProcessingEngine engine) {
		ResultSet<BlockedMatchable<RecordType, SchemaElementType>> result = block(dataset1, dataset2, schemaCorrespondences); 
		
		calculatePerformance(dataset1, dataset2, result);
		
		return result;
	}
	
	@Override
	public ResultSet<BlockedMatchable<RecordType, SchemaElementType>> runBlocking(
			DataSet<RecordType, SchemaElementType> dataset, boolean isSymmetric,
			ResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences,
			DataProcessingEngine engine) {
		ResultSet<BlockedMatchable<RecordType, SchemaElementType>> result = block(dataset, isSymmetric, schemaCorrespondences);
		
		calculatePerformance(dataset, dataset, result);
		
		return result;
	}
	
	/**
	 * Generates the pairs of {@link Record}s between two {@link DefaultDataSet}s that
	 * should be compared according to this blocking strategy.
	 * 
	 * @param dataset1
	 *            the first data set
	 * @param dataset2
	 *            the second data set
	 * @return the list of pairs that resulted from the blocking
	 */
	public abstract ResultSet<BlockedMatchable<RecordType, SchemaElementType>> block(
			DataSet<RecordType, SchemaElementType> dataset1, DataSet<RecordType, SchemaElementType> dataset2, ResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences);

	/**
	 * Generates the pairs of {@link Record}s within a {@link DefaultDataSet} that
	 * should be compared according to this blocking strategy.
	 * 
	 * @param dataset
	 *            the dataset including the {@link Record}s which should be
	 *            compared.
	 * @param isSymmetric
	 *            states if it can be assumed that the later comparison of a and
	 *            b is equal to the comparison of b and a. In most cases (using
	 *            most {@link SimilarityMeasure}) this will be true.
	 * @return
	 */
	public abstract ResultSet<BlockedMatchable<RecordType, SchemaElementType>> block(
			DataSet<RecordType, SchemaElementType> dataset, boolean isSymmetric, ResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences);
	
	
}
