package de.uni_mannheim.informatik.wdi.matching.blocking;

import java.io.Serializable;

import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.DataSet;
import de.uni_mannheim.informatik.wdi.model.Matchable;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;

public abstract class SchemaBlocker<SchemaElementType extends Matchable, RecordType extends Matchable> implements Serializable{
	

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private double reductionRatio = 1.0;

	/**
	 * Returns the reduction ratio of the last blocking operation. Only
	 * available after calculatePerformance(...) has been called.
	 * 
	 * @return the reduction ratio
	 */
	public double getReductionRatio() {
		return reductionRatio;
	}
	
	/**
	 * Calculates the reduction ratio. Must be called by all sub classes in
	 * generatePairs(...).
	 * 
	 * @param dataset1
	 *            the first data set
	 * @param dataset2
	 *            the second data set
	 * @param blockedPairs
	 *            the list of pairs that resulted from the blocking
	 */
	protected void calculatePerformance(DataSet<SchemaElementType, SchemaElementType> schema1,
			DataSet<SchemaElementType, SchemaElementType> schema2,
			ResultSet<BlockedMatchable<SchemaElementType, RecordType>> blocked) {
		long maxPairs = (long) schema1.getSize() * (long) schema2.getSize();

//		reductionRatio = (double) maxPairs / (double) blocked.size();
		reductionRatio = 1.0 - ((double)blocked.size() / (double)maxPairs);
	}
	
	public abstract void initialise(DataSet<SchemaElementType, SchemaElementType> schema1, DataSet<SchemaElementType, SchemaElementType> schema2,
			ResultSet<Correspondence<RecordType, SchemaElementType>> instanceCorrespondences);
	
	public abstract void initialise(DataSet<SchemaElementType, SchemaElementType> dataset, boolean isSymmetric,
			ResultSet<Correspondence<RecordType, SchemaElementType>> instanceCorrespondences);
	
	public abstract ResultSet<BlockedMatchable<SchemaElementType, RecordType>> runBlocking(
			DataSet<SchemaElementType, SchemaElementType> schema1, DataSet<SchemaElementType, SchemaElementType> schema2,
			ResultSet<Correspondence<RecordType, SchemaElementType>> instanceCorrespondences,
			DataProcessingEngine engine);// {
//		
//		ResultSet<BlockedMatchable<SchemaElementType, RecordType>> result = engine.runSchemaBlocking(schema1, schema2, instanceCorrespondences, this); 
//		
//		calculatePerformance(schema1, schema2, result);
//		
//		return result;
//	}
//	
//	public ResultSet<BlockedMatchable<SchemaElementType, RecordType>> runBlocking(
//			DataSet<SchemaElementType, SchemaElementType> schema, boolean isSymmetric,
//			ResultSet<Correspondence<RecordType, SchemaElementType>> instanceCorrespondences,
//			MatchingEngine<RecordType, SchemaElementType> engine) {
//		
//		ResultSet<BlockedMatchable<SchemaElementType, RecordType>> result = engine.runSchemaBlocking(schema, isSymmetric, instanceCorrespondences, this);
//		
//		calculatePerformance(schema, schema, result);
//		
//		return result;
//	}
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
	//TODO this version of the method might be unnecessary
//	public abstract ResultSet<BlockedMatchable<SchemaElementType, RecordType>> block(
//			SchemaElementType record, DataSet<SchemaElementType, SchemaElementType> dataset, ResultSet<Correspondence<RecordType, SchemaElementType>> instanceCorrespondences);
//	
//	public abstract ResultSet<BlockedMatchable<SchemaElementType, RecordType>> block(
//			SchemaElementType record, DataSet<SchemaElementType, SchemaElementType> dataset, boolean isSymmetric, ResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences);
}
