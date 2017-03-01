package de.uni_mannheim.informatik.wdi.parallel;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.joda.time.DateTime;

import de.uni_mannheim.informatik.dws.t2k.utils.concurrent.Consumer;
import de.uni_mannheim.informatik.dws.t2k.utils.concurrent.Parallel;
import de.uni_mannheim.informatik.wdi.matching.MatchingEngine;
import de.uni_mannheim.informatik.wdi.matching.MatchingRule;
import de.uni_mannheim.informatik.wdi.matching.SchemaMatchingRule;
import de.uni_mannheim.informatik.wdi.matching.SchemaMatchingRuleWithVoting;
import de.uni_mannheim.informatik.wdi.matching.blocking.BlockedMatchable;
import de.uni_mannheim.informatik.wdi.matching.blocking.Blocker;
import de.uni_mannheim.informatik.wdi.matching.blocking.SchemaBlocker;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.DataSet;
import de.uni_mannheim.informatik.wdi.model.DefaultDataSet;
import de.uni_mannheim.informatik.wdi.model.DefaultRecord;
import de.uni_mannheim.informatik.wdi.model.FeatureVectorDataSet;
import de.uni_mannheim.informatik.wdi.model.Matchable;
import de.uni_mannheim.informatik.wdi.model.MatchingGoldStandard;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.Record;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.similarity.string.LevenshteinSimilarity;
import de.uni_mannheim.informatik.wdi.similarity.string.TokenizingJaccardSimilarity;
import de.uni_mannheim.informatik.wdi.utils.ProgressReporter;

public class ParallelMatchingEngine<RecordType extends Matchable, SchemaElementType extends Matchable> extends MatchingEngine<RecordType, SchemaElementType> {

	public ParallelMatchingEngine() {
		super(new ParallelDataProcessingEngine());
	}
	
	private int maxParallel = 0;
	
	public void setMaxParallel(int maxParallel) {
		this.maxParallel = maxParallel;
	}
	
	/**
	 * Runs the Duplicate Detection on a given {@link DefaultDataSet}. In order to
	 * reduce the number of internally compared {@link Record}s the functions
	 * can be executed in a <i>symmetric</i>-mode. Here it will be assumed, that
	 * that the {@link MatchingRule} is symmetric, meaning that the score(a,b) =
	 * score(b,a). Therefore the pair (b,a) can be left out. Normally, this
	 * option can be set to <b>true</b> in most of the cases, as most of the
	 * common similarity functions (e.g. {@link LevenshteinSimilarity}, and
	 * {@link TokenizingJaccardSimilarity}) are symmetric, meaning sim(a,b) =
	 * sim(b,a).
	 * 
	 * @param dataset
	 *            The data set
	 * @param symmetric
	 *            indicates of the used {@link MatchingRule} is symmetric,
	 *            meaning that the order of elements does not matter.
	 * @return A list of correspondences
	 */
	@Override
	public ResultSet<Correspondence<RecordType, SchemaElementType>> runDuplicateDetection(
			DataSet<RecordType, SchemaElementType> dataset, 
			boolean symmetric, 
			ResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences,
			final MatchingRule<RecordType, SchemaElementType> rule,
			Blocker<RecordType, SchemaElementType> blocker) {
		long start = System.currentTimeMillis();

		System.out.println(String.format("[%s] Starting Duplicate Detection",
				new DateTime(start).toString()));

		final ResultSet<Correspondence<RecordType, SchemaElementType>> result = new ThreadSafeResultSet<>();

		System.out.println(String.format("Blocking %,d x %,d elements", dataset.getSize(), dataset.getSize()));
		
		// use the blocker to generate pairs
		ResultSet<BlockedMatchable<RecordType, SchemaElementType>> allPairs = blocker.runBlocking(dataset, symmetric, schemaCorrespondences, getProcessingEngine());

		System.out
				.println(String
						.format("Duplicate Detection %,d x %,d elements; %,d blocked pairs (reduction ratio: %.2f)",
								dataset.getSize(), dataset.getSize(),
								allPairs.size(), blocker.getReductionRatio()));

		// compare the pairs using the Duplicate Detection rule
		new Parallel<BlockedMatchable<RecordType, SchemaElementType>>(maxParallel).tryForeach(allPairs.get(), new Consumer<BlockedMatchable<RecordType, SchemaElementType>>() {

			@Override
			public void execute(
					BlockedMatchable<RecordType, SchemaElementType> task) {
				// apply the Duplicate Detection rule
				Correspondence<RecordType, SchemaElementType> cor = rule.apply(task.getFirstRecord(), task.getSecondRecord(), task.getSchemaCorrespondences());
				if (cor != null) {

					// add the correspondences to the result
					result.add(cor);
				}				
			}
			
		});

		// report total Duplicate Detection time
		long end = System.currentTimeMillis();
		long delta = end - start;
		System.out
				.println(String
						.format("[%s] Duplicate Detection finished after %s; found %,d correspondences.",
								new DateTime(end).toString(),
								DurationFormatUtils.formatDurationHMS(delta),
								result.size()));

		return result;
	}

	/**
	 * Runs the matching on the given data sets
	 * 
	 * @param dataset1
	 *            The first data set
	 * @param dataset2
	 *            The second data set
	 * @return A list of correspondences
	 */
	@Override
	public ResultSet<Correspondence<RecordType, SchemaElementType>> runIdentityResolution(
			DataSet<RecordType, SchemaElementType> dataset1, 
			DataSet<RecordType, SchemaElementType> dataset2, 
			ResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences,
			final MatchingRule<RecordType, SchemaElementType> rule,
			Blocker<RecordType, SchemaElementType> blocker) {
		long start = System.currentTimeMillis();

		System.out.println(String.format("[%s] Starting Identity Resolution",
				new DateTime(start).toString()));

		final ResultSet<Correspondence<RecordType, SchemaElementType>> result = new ThreadSafeResultSet<>();

		System.out.println(String.format("Blocking %,d x %,d elements", dataset1.getSize(), dataset2.getSize()));
		
		// use the blocker to generate pairs
		ResultSet<BlockedMatchable<RecordType, SchemaElementType>> allPairs = blocker.runBlocking(dataset1, dataset2, schemaCorrespondences, getProcessingEngine());

		System.out
				.println(String
						.format("Matching %,d x %,d elements; %,d blocked pairs (reduction ratio: %,.2f)",
								dataset1.getSize(), dataset2.getSize(),
								allPairs.size(), blocker.getReductionRatio()));
		
		// compare the pairs using the matching rule
		new Parallel<BlockedMatchable<RecordType, SchemaElementType>>(maxParallel).tryForeach(allPairs.get(), new Consumer<BlockedMatchable<RecordType, SchemaElementType>>() {

			@Override
			public void execute(
					BlockedMatchable<RecordType, SchemaElementType> task) {
				// apply the matching rule
				Correspondence<RecordType, SchemaElementType> cor = rule.apply(task.getFirstRecord(), task.getSecondRecord(), task.getSchemaCorrespondences());
				if (cor != null) {

					// add the correspondences to the result
					result.add(cor);
				}
			}
			
		});
		
		// report total matching time
		long end = System.currentTimeMillis();
		long delta = end - start;
		System.out.println(String.format(
				"[%s] Identity Resolution finished after %s; found %,d correspondences.",
				new DateTime(end).toString(),
				DurationFormatUtils.formatDurationHMS(delta), result.size()));

		return result;
	}

	/**
	 * Generates a data set containing features that can be used to learn
	 * matching rules.
	 * 
	 * @param dataset1
	 *            The first data set
	 * @param dataset2
	 *            The second data set
	 * @param goldStandard
	 *            The gold standard containing the labels for the generated data
	 *            set
	 * @return
	 */
	@Override
	public void generateTrainingDataForLearning(
			DataSet<RecordType, SchemaElementType> dataset1, 
			DataSet<RecordType, SchemaElementType> dataset2,
			MatchingGoldStandard goldStandard,
			MatchingRule<RecordType, SchemaElementType> rule,
			ResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences,
			FeatureVectorDataSet result) {
		long start = System.currentTimeMillis();

		goldStandard.printBalanceReport();

		System.out.println(String.format("[%s] Starting GenerateFeatures",
				new DateTime(start).toString()));

		ProgressReporter progress = new ProgressReporter(goldStandard
				.getPositiveExamples().size()
				+ goldStandard.getNegativeExamples().size(), "GenerateFeatures");

		// create positive examples
		for (Pair<String, String> correspondence : goldStandard
				.getPositiveExamples()) {
			RecordType record1 = dataset1.getRecord(correspondence.getFirst());
			RecordType record2 = dataset2.getRecord(correspondence.getSecond());

			// we don't know which id is from which data set
			if (record1 == null && record2 == null) {
				// so if we didn't find anything, we probably had it wrong ...
				record1 = dataset2.getRecord(correspondence.getFirst());
				record2 = dataset1.getRecord(correspondence.getSecond());
			}

			DefaultRecord features = rule.generateFeatures(record1, record2, schemaCorrespondences, result);
			features.setValue(FeatureVectorDataSet.ATTRIBUTE_LABEL, "1");
			result.addRecord(features);

			// increment and report status
			progress.incrementProgress();
			progress.report();
		}

		// create negative examples
		for (Pair<String, String> correspondence : goldStandard
				.getNegativeExamples()) {
			RecordType record1 = dataset1.getRecord(correspondence.getFirst());
			RecordType record2 = dataset2.getRecord(correspondence.getSecond());

			// we don't know which id is from which data set
			if (record1 == null && record2 == null) {
				// so if we didn't find anything, we probably had it wrong ...
				record1 = dataset2.getRecord(correspondence.getFirst());
				record2 = dataset1.getRecord(correspondence.getSecond());
			}

			DefaultRecord features = rule.generateFeatures(record1, record2, schemaCorrespondences, result);
			features.setValue(FeatureVectorDataSet.ATTRIBUTE_LABEL, "0");
			result.addRecord(features);

			// increment and report status
			progress.incrementProgress();
			progress.report();
		}

		// report total time
		long end = System.currentTimeMillis();
		long delta = end - start;
		System.out
				.println(String
						.format("[%s] GenerateFeatures finished after %s; created %,d examples.",
								new DateTime(end).toString(),
								DurationFormatUtils.formatDurationHMS(delta),
								result.getSize()));
	}

	
	public ResultSet<Correspondence<SchemaElementType, RecordType>> runSchemaMatching(
			DataSet<SchemaElementType, SchemaElementType> schema1, 
			DataSet<SchemaElementType, SchemaElementType> schema2,
			ResultSet<Correspondence<RecordType, SchemaElementType>> instanceCorrespondences,
			final MatchingRule<SchemaElementType, RecordType> rule,
			SchemaBlocker<SchemaElementType, RecordType> blocker) {
		long start = System.currentTimeMillis();

		System.out.println(String.format("[%s] Starting Schema Matching",
				new DateTime(start).toString()));

		final ResultSet<Correspondence<SchemaElementType, RecordType>> result = new ThreadSafeResultSet<>();

		System.out.println(String.format("Blocking %,d x %,d elements", schema1.getSize(), schema2.getSize()));
		
		// use the blocker to generate pairs
		ResultSet<BlockedMatchable<SchemaElementType, RecordType>> allPairs = blocker.runBlocking(schema1, schema2, instanceCorrespondences, getProcessingEngine());

		System.out
				.println(String
						.format("Matching %,d x %,d elements; %,d blocked pairs (reduction ratio: %s)",
								schema1.getSize(), schema2.getSize(),
								allPairs.size(), Double.toString(blocker.getReductionRatio())));
		
		new Parallel<BlockedMatchable<SchemaElementType, RecordType>>(maxParallel).tryForeach(allPairs.get(), new Consumer<BlockedMatchable<SchemaElementType, RecordType>>(){

			@Override
			public void execute(
					BlockedMatchable<SchemaElementType, RecordType> task) {
				// apply the matching rule
				Correspondence<SchemaElementType, RecordType> cor = rule.apply(task.getFirstRecord(), task.getSecondRecord(), task.getSchemaCorrespondences());
				if (cor != null) {

					// add the correspondences to the result
					result.add(cor);
				}
			}}, "Schema Matching");

		// report total matching time
		long end = System.currentTimeMillis();
		long delta = end - start;
		System.out.println(String.format(
				"[%s] Identity Resolution finished after %s; found %,d correspondences.",
				new DateTime(end).toString(),
				DurationFormatUtils.formatDurationHMS(delta), result.size()));

		return result;
	}
	
	/**
	 * Runs the matching on the given data sets
	 * 
	 * @param dataset1
	 *            The first data set
	 * @param dataset2
	 *            The second data set
	 * @return A list of correspondences
	 */
	@Override
	public ResultSet<Correspondence<SchemaElementType, RecordType>> runSchemaMatching(
			final DataSet<SchemaElementType, SchemaElementType> schema1, final DataSet<SchemaElementType, SchemaElementType> schema2,
			ResultSet<Correspondence<RecordType, SchemaElementType>> instanceCorrespondences,
			final SchemaMatchingRule<RecordType, SchemaElementType, SchemaElementType> rule) {
		long start = System.currentTimeMillis();

		System.out.println(String.format("[%s] Starting Schema Matching",
				new DateTime(start).toString()));

		final ResultSet<Correspondence<SchemaElementType, RecordType>> result = new ThreadSafeResultSet<>();

		System.out
				.println(String
						.format("Matching %,d elements",
								instanceCorrespondences.size()));

		// compare the pairs using the matching rule
		new Parallel<Correspondence<RecordType, SchemaElementType>>(maxParallel).tryForeach(instanceCorrespondences.get(), new Consumer<Correspondence<RecordType, SchemaElementType>>() {

			@Override
			public void execute(Correspondence<RecordType, SchemaElementType> correspondence) {
				// apply the matching rule
				ResultSet<Correspondence<SchemaElementType, RecordType>> cor = rule.apply(schema1, schema2, correspondence);
				if (cor != null) {

					// add the correspondences to the result
					result.merge(cor);
				}
			}
			
		});

		// report total matching time
		long end = System.currentTimeMillis();
		long delta = end - start;
		System.out.println(String.format(
				"[%s] Schema Matching finished after %s; found %d correspondences.",
				new DateTime(end).toString(),
				DurationFormatUtils.formatDurationHMS(delta), result.size()));

		return result;
	}
	
	/**
	 * Runs the matching on the given data sets
	 * 
	 * @param dataset1
	 *            The first data set
	 * @param dataset2
	 *            The second data set
	 * @return A list of correspondences
	 */
	@Override
	public ResultSet<Correspondence<SchemaElementType, RecordType>> runDuplicateBasedSchemaMatching(
			final DataSet<SchemaElementType, SchemaElementType> schema1, final DataSet<SchemaElementType, SchemaElementType> schema2,
			ResultSet<Correspondence<RecordType, SchemaElementType>> instanceCorrespondences,
			final SchemaMatchingRuleWithVoting<RecordType, SchemaElementType, SchemaElementType> rule,
			SchemaBlocker<SchemaElementType, RecordType> schemaBlocker) {
		long start = System.currentTimeMillis();

		System.out.println(String.format("[%s] Starting Duplicate-based Schema Matching",
				new DateTime(start).toString()));

		final ResultSet<Pair<Correspondence<RecordType, SchemaElementType>,Correspondence<SchemaElementType, RecordType>>> result = new ThreadSafeResultSet<>();

		System.out.println(String.format("Blocking %,d x %,d elements", schema1.getSize(), schema2.getSize()));
		
		ResultSet<BlockedMatchable<SchemaElementType, RecordType>> blocked = schemaBlocker.runBlocking(schema1, schema2, instanceCorrespondences, getProcessingEngine());
		
		System.out
		.println(String
				.format("Matching %,d x %,d elements; %,d blocked pairs (reduction ratio: %s)",
						schema1.getSize(), schema2.getSize(),
						blocked.size(), Double.toString(schemaBlocker.getReductionRatio())));
		
		// compare the pairs using the matching rule
		new Parallel<BlockedMatchable<SchemaElementType, RecordType>>(maxParallel).tryForeach(blocked.get(), new Consumer<BlockedMatchable<SchemaElementType, RecordType>>() {

			@Override
			public void execute(BlockedMatchable<SchemaElementType, RecordType> blockedMatchable) {
				for (Correspondence<RecordType, SchemaElementType> correspondence : blockedMatchable.getSchemaCorrespondences().get()) {
					// apply the matching rule
					Correspondence<SchemaElementType, RecordType> c = rule.apply(blockedMatchable.getFirstRecord(), blockedMatchable.getSecondRecord(), correspondence);
					
					if (c != null) {
	
						Pair<Correspondence<RecordType, SchemaElementType>,Correspondence<SchemaElementType, RecordType>> cor = new Pair<>(correspondence, c);
						// add the correspondences to the result
						result.add(cor);
					}
				}
			}
			
		});
		


		// aggregate results: voting
		ResultSet<Correspondence<SchemaElementType, RecordType>> finalResult = rule.aggregate(result, instanceCorrespondences.size(), getProcessingEngine());
		
		// report total matching time
		long end = System.currentTimeMillis();
		long delta = end - start;
		System.out.println(String.format(
				"[%s] Duplicate-based Schema Matching finished after %s; found %d correspondences from %,d elements.",
				new DateTime(end).toString(),
				DurationFormatUtils.formatDurationHMS(delta), finalResult.size(), result.size()));

		return finalResult;
	}
	
//	@Override
//	public ResultSet<BlockedMatchable<SchemaElementType, RecordType>> runSchemaBlocking(
//			DataSet<SchemaElementType, SchemaElementType> dataset1, 
//			final DataSet<SchemaElementType, SchemaElementType> dataset2, 
//			final ResultSet<Correspondence<RecordType, SchemaElementType>> instanceCorrespondences,
//			final SchemaBlocker<SchemaElementType, RecordType> blocker) {
//		
//		final ResultSet<BlockedMatchable<SchemaElementType, RecordType>> result = new ThreadSafeResultSet<>();
//		
//		blocker.initialise(dataset1, dataset2, instanceCorrespondences);
//		
//		new Parallel<SchemaElementType>(maxParallel).tryForeach(dataset1.getRecords(), new Consumer<SchemaElementType>() {
//
//			@Override
//			public void execute(SchemaElementType r) {
//				ResultSet<BlockedMatchable<SchemaElementType, RecordType>> intermediateResult = blocker.block(r, dataset2, instanceCorrespondences);
//				
//				for(BlockedMatchable<SchemaElementType, RecordType> b : intermediateResult.get()) {
//					result.add(b);
//				}
//			}}, "Blocking");
//
//		return result;
//	}
	
//	@Override
//	public ResultSet<BlockedMatchable<RecordType, SchemaElementType>> runBlocking(
//			final DataSet<RecordType, SchemaElementType> dataset, final boolean isSymmetric,
//			final ResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences,
//			final RecordLevelBlocker<RecordType, SchemaElementType> blocker) {
//		
//		final ResultSet<BlockedMatchable<RecordType, SchemaElementType>> result = new ThreadSafeResultSet<>();
//		
//		blocker.initialise(dataset, isSymmetric, schemaCorrespondences);
//		
//		
//		new Parallel<RecordType>(maxParallel).tryForeach(dataset.getRecords(), new Consumer<RecordType>() {
//
//			@Override
//			public void execute(RecordType record) {
//				ResultSet<BlockedMatchable<RecordType, SchemaElementType>> intermediateResult = blocker.block(record, dataset, isSymmetric, schemaCorrespondences);
//				
//				for(BlockedMatchable<RecordType, SchemaElementType> b : intermediateResult.get()) {
//					result.add(b);
//				}
//			}
//			
//		}, "Blocking");
//
//		return result;
//	}
//	
//	@Override
//	public ResultSet<BlockedMatchable<RecordType, SchemaElementType>> runBlocking(
//			DataSet<RecordType, SchemaElementType> dataset1, final DataSet<RecordType, SchemaElementType> dataset2,
//			final ResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences,
//			final RecordLevelBlocker<RecordType, SchemaElementType> blocker) {
//		final ResultSet<BlockedMatchable<RecordType, SchemaElementType>> result = new ThreadSafeResultSet<>();
//		
//		blocker.initialise(dataset1, dataset2, schemaCorrespondences);
//		
//		new Parallel<RecordType>(maxParallel).tryForeach(dataset1.getRecords(), new Consumer<RecordType>() {
//
//			@Override
//			public void execute(RecordType record) {
//				ResultSet<BlockedMatchable<RecordType, SchemaElementType>> intermediateResult = blocker.block(record, dataset2, schemaCorrespondences);
//				
//				for(BlockedMatchable<RecordType, SchemaElementType> b : intermediateResult.get()) {
//					result.add(b);
//				}				
//			}
//			
//		}, "Blocking");
//
//		return result;
//	}
}
