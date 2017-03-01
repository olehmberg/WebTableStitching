package de.uni_mannheim.informatik.wdi.matching.blocking;

import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.DataSet;
import de.uni_mannheim.informatik.wdi.model.DefaultDataSet;
import de.uni_mannheim.informatik.wdi.model.Matchable;
import de.uni_mannheim.informatik.wdi.model.Record;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;
import de.uni_mannheim.informatik.wdi.processing.DatasetIterator;
import de.uni_mannheim.informatik.wdi.processing.RecordMapper;
import de.uni_mannheim.informatik.wdi.similarity.SimilarityMeasure;

/**
 * 
 * Deprecated. Use {@link StandardBlocker} instead.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 * @param <RecordType>
 * @param <SchemaElementType>
 */
@Deprecated
public abstract class RecordLevelBlocker<RecordType extends Matchable, SchemaElementType extends Matchable> extends Blocker<RecordType, SchemaElementType> {
	
	public abstract void initialise(DataSet<RecordType, SchemaElementType> dataset1, DataSet<RecordType, SchemaElementType> dataset2,
			ResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences);
	
	public abstract void initialise(DataSet<RecordType, SchemaElementType> dataset, boolean isSymmetric,
			ResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences);
	
	@Override
	public ResultSet<BlockedMatchable<RecordType, SchemaElementType>> runBlocking(
			DataSet<RecordType, SchemaElementType> dataset1, 
			final DataSet<RecordType, SchemaElementType> dataset2,
			final ResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences,
			DataProcessingEngine engine) {
		
		ResultSet<BlockedMatchable<RecordType, SchemaElementType>> result = null; //engine.runBlocking(dataset1, dataset2, schemaCorrespondences, this); 
		
		initialise(dataset1, dataset2, schemaCorrespondences);

		result = engine.transform(dataset1, new RecordMapper<RecordType, BlockedMatchable<RecordType, SchemaElementType>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(
					RecordType record,
					DatasetIterator<BlockedMatchable<RecordType, SchemaElementType>> resultCollector) {
				ResultSet<BlockedMatchable<RecordType, SchemaElementType>> intermediateResult = block(record, dataset2, schemaCorrespondences);
			
				for(BlockedMatchable<RecordType, SchemaElementType> b : intermediateResult.get()) {
					resultCollector.next(b);
				}
			}
			
		});
		
		calculatePerformance(dataset1, dataset2, result);
		
		return result;
	}
	
	@Override
	public ResultSet<BlockedMatchable<RecordType, SchemaElementType>> runBlocking(
			final DataSet<RecordType, SchemaElementType> dataset, 
			final boolean isSymmetric,
			final ResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences,
			DataProcessingEngine engine) {
		
		ResultSet<BlockedMatchable<RecordType, SchemaElementType>> result = null;
		
		initialise(dataset, isSymmetric, schemaCorrespondences);
		
		result = engine.transform(dataset, new RecordMapper<RecordType, BlockedMatchable<RecordType, SchemaElementType>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(
					RecordType record,
					DatasetIterator<BlockedMatchable<RecordType, SchemaElementType>> resultCollector) {
				ResultSet<BlockedMatchable<RecordType, SchemaElementType>> intermediateResult = block(record, dataset, isSymmetric, schemaCorrespondences);
				
				for(BlockedMatchable<RecordType, SchemaElementType> b : intermediateResult.get()) {
					resultCollector.next(b);
				}
			}
		});

		calculatePerformance(dataset, dataset, result);
		
		return result;
	}
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
	public abstract ResultSet<BlockedMatchable<RecordType, SchemaElementType>> block(
			RecordType record, DataSet<RecordType, SchemaElementType> dataset, ResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences);
	
	public abstract ResultSet<BlockedMatchable<RecordType, SchemaElementType>> block(
			RecordType record, DataSet<RecordType, SchemaElementType> dataset, boolean isSymmetric, ResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences);
}
