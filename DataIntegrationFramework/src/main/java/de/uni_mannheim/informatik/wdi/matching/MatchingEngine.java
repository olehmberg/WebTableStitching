/** 
 *
 * Copyright (C) 2015 Data and Web Science Group, University of Mannheim, Germany (code@dwslab.de)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 		http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package de.uni_mannheim.informatik.wdi.matching;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.joda.time.DateTime;

import au.com.bytecode.opencsv.CSVWriter;
import de.uni_mannheim.informatik.wdi.matching.blocking.BlockedMatchable;
import de.uni_mannheim.informatik.wdi.matching.blocking.Blocker;
import de.uni_mannheim.informatik.wdi.matching.blocking.SchemaBlocker;
import de.uni_mannheim.informatik.wdi.matching.blocking.StandardSchemaBlocker;
import de.uni_mannheim.informatik.wdi.matching.blocking.StaticBlockingKeyGenerator;
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
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;
import de.uni_mannheim.informatik.wdi.similarity.string.LevenshteinSimilarity;
import de.uni_mannheim.informatik.wdi.similarity.string.TokenizingJaccardSimilarity;
import de.uni_mannheim.informatik.wdi.utils.ProgressReporter;

/**
 * The matching engine that executes a given {@link MatchingRule} on one or two
 * {@link DefaultDataSet}s. In the first case, duplicate detection is performed. In the
 * second identity resolution is performed.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 * @author Robert Meusel (robert@dwslab.de)
 * 
 * @param <RecordType>
 */
public class MatchingEngine<RecordType extends Matchable, SchemaElementType extends Matchable>  {
	
	public MatchingEngine() {
		this.processingEngine = new DataProcessingEngine();
	}
	
	public MatchingEngine(DataProcessingEngine processingEngine) {
		this.processingEngine = processingEngine;
	}
	
	protected DataProcessingEngine processingEngine;
	
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
	public ResultSet<Correspondence<RecordType, SchemaElementType>> runDuplicateDetection(
			DataSet<RecordType, SchemaElementType> dataset, 
			boolean symmetric, 
			ResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences,
			MatchingRule<RecordType, SchemaElementType> rule,
			Blocker<RecordType, SchemaElementType> blocker) {
		long start = System.currentTimeMillis();

		System.out.println(String.format("[%s] Starting Duplicate Detection",
				new DateTime(start).toString()));

		ResultSet<Correspondence<RecordType, SchemaElementType>> result = new ResultSet<>();

		System.out.println(String.format("Blocking %,d x %,d elements", dataset.getSize(), dataset.getSize()));
		// use the blocker to generate pairs
		//TODO don't generate pairs here! just generate blocks, then generate pairs from the blocks inside the next loop (otherwise number of pairs can be too large to handle)
		ResultSet<BlockedMatchable<RecordType, SchemaElementType>> allPairs = blocker.runBlocking(dataset, symmetric, schemaCorrespondences, getProcessingEngine());

		System.out
				.println(String
						.format("Duplicate Detection %,d x %,d elements; %,d blocked pairs (reduction ratio: %.2f)",
								dataset.getSize(), dataset.getSize(),
								allPairs.size(), blocker.getReductionRatio()));

		// compare the pairs using the Duplicate Detection rule
		ProgressReporter progress = new ProgressReporter(allPairs.size(),
				"Duplicate Detection");
		for(BlockedMatchable<RecordType, SchemaElementType> task : allPairs.get()) {

			// apply the Duplicate Detection rule
			Correspondence<RecordType, SchemaElementType> cor = rule.apply(task.getFirstRecord(), task.getSecondRecord(), task.getSchemaCorrespondences());
			if (cor != null) {

				// add the correspondences to the result
				result.add(cor);
			}

			// increment and report status
			progress.incrementProgress();
			progress.report();
		}

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
	public ResultSet<Correspondence<RecordType, SchemaElementType>> runIdentityResolution(
			DataSet<RecordType, SchemaElementType> dataset1, 
			DataSet<RecordType, SchemaElementType> dataset2, 
			ResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences,
			MatchingRule<RecordType, SchemaElementType> rule,
			Blocker<RecordType, SchemaElementType> blocker) {
		long start = System.currentTimeMillis();

		System.out.println(String.format("[%s] Starting Identity Resolution",
				new DateTime(start).toString()));

		ResultSet<Correspondence<RecordType, SchemaElementType>> result = new ResultSet<>();

		System.out.println(String.format("Blocking %,d x %,d elements", dataset1.getSize(), dataset2.getSize()));
		
		// use the blocker to generate pairs
		ResultSet<BlockedMatchable<RecordType, SchemaElementType>> allPairs = blocker.runBlocking(dataset1, dataset2, schemaCorrespondences, getProcessingEngine());

		System.out
				.println(String
						.format("Matching %,d x %,d elements; %,d blocked pairs (reduction ratio: %s)",
								dataset1.getSize(), dataset2.getSize(),
								allPairs.size(), Double.toString(blocker.getReductionRatio())));

		// compare the pairs using the matching rule
		ProgressReporter progress = new ProgressReporter(allPairs.size(),
				"Identity Resolution");
		for(BlockedMatchable<RecordType, SchemaElementType> task : allPairs.get()) {

			// apply the matching rule
			Correspondence<RecordType, SchemaElementType> cor = rule.apply(task.getFirstRecord(), task.getSecondRecord(), task.getSchemaCorrespondences());
			if (cor != null) {

				// add the correspondences to the result
				result.add(cor);
			}

			// increment and report status
			progress.incrementProgress();
			progress.report();
		}

		// report total matching time
		long end = System.currentTimeMillis();
		long delta = end - start;
		System.out.println(String.format(
				"[%s] Identity Resolution finished after %s; found %,d correspondences.",
				new DateTime(end).toString(),
				DurationFormatUtils.formatDurationHMS(delta), result.size()));

		return result;
	}
	
//	public ResultSet<BlockedMatchable<SchemaElementType, RecordType>> runSchemaBlocking(
//			DataSet<SchemaElementType, SchemaElementType> dataset1, 
//			DataSet<SchemaElementType, SchemaElementType> dataset2, 
//			ResultSet<Correspondence<RecordType, SchemaElementType>> instanceCorrespondences,
//			SchemaBlocker<SchemaElementType, RecordType> blocker) {
//		
//		ResultSet<BlockedMatchable<SchemaElementType, RecordType>> result = new ResultSet<>();
//		
//		blocker.initialise(dataset1, dataset2, instanceCorrespondences);
//		
//		for(SchemaElementType r : dataset1.getRecords()) {
//			ResultSet<BlockedMatchable<SchemaElementType, RecordType>> intermediateResult = blocker.block(r, dataset2, instanceCorrespondences);
//			
//			for(BlockedMatchable<SchemaElementType, RecordType> b : intermediateResult.get()) {
//				result.add(b);
//			}
//		}
//
//		return result;
//	}
	
//	public ResultSet<BlockedMatchable<RecordType, SchemaElementType>> runBlocking(
//			DataSet<RecordType, SchemaElementType> dataset1, 
//			DataSet<RecordType, SchemaElementType> dataset2, 
//			ResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences,
//			RecordLevelBlocker<RecordType, SchemaElementType> blocker) {
//		
//		ResultSet<BlockedMatchable<RecordType, SchemaElementType>> result = new ResultSet<>();
//		
//		blocker.initialise(dataset1, dataset2, schemaCorrespondences);
//		
//		for(RecordType r : dataset1.getRecords()) {
//			ResultSet<BlockedMatchable<RecordType, SchemaElementType>> intermediateResult = blocker.block(r, dataset2, schemaCorrespondences);
//			
//			for(BlockedMatchable<RecordType, SchemaElementType> b : intermediateResult.get()) {
//				result.add(b);
//			}
//		}
//
//		return result;
//	}
	
//	public ResultSet<BlockedMatchable<RecordType, SchemaElementType>> runBlocking(
//			DataSet<RecordType, SchemaElementType> dataset, 
//			boolean isSymmetric, 
//			ResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences,
//			RecordLevelBlocker<RecordType, SchemaElementType> blocker) {
//		
//		ResultSet<BlockedMatchable<RecordType, SchemaElementType>> result = new ResultSet<>();
//		
//		blocker.initialise(dataset, isSymmetric, schemaCorrespondences);
//		
//		for(RecordType r : dataset.getRecords()) {
//			ResultSet<BlockedMatchable<RecordType, SchemaElementType>> intermediateResult = blocker.block(r, dataset, isSymmetric, schemaCorrespondences);
//			
//			for(BlockedMatchable<RecordType, SchemaElementType> b : intermediateResult.get()) {
//				result.add(b);
//			}
//		}
//
//		return result;
//	}
	
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

	public <A extends Matchable, B> void writeCorrespondences(
			Collection<Correspondence<A, B>> correspondences, File file)
			throws IOException {
		CSVWriter w = new CSVWriter(new FileWriter(file));

		for (Correspondence<A, B> c : correspondences) {
			w.writeNext(new String[] { c.getFirstRecord().getIdentifier(),
					c.getSecondRecord().getIdentifier(),
					Double.toString(c.getSimilarityScore()) });
		}

		w.close();
	}

	public ResultSet<Correspondence<SchemaElementType, RecordType>> runSchemaMatching(
			DataSet<SchemaElementType, SchemaElementType> schema1, 
			DataSet<SchemaElementType, SchemaElementType> schema2,
			ResultSet<Correspondence<RecordType, SchemaElementType>> instanceCorrespondences,
			MatchingRule<SchemaElementType, RecordType> rule,
			SchemaBlocker<SchemaElementType, RecordType> blocker) {
		long start = System.currentTimeMillis();

		System.out.println(String.format("[%s] Starting Schema Matching",
				new DateTime(start).toString()));

		ResultSet<Correspondence<SchemaElementType, RecordType>> result = new ResultSet<>();

		System.out.println(String.format("Blocking %,d x %,d elements", schema1.getSize(), schema2.getSize()));
		
		// use the blocker to generate pairs
		ResultSet<BlockedMatchable<SchemaElementType, RecordType>> allPairs = blocker.runBlocking(schema1, schema2, instanceCorrespondences, getProcessingEngine());

		System.out
				.println(String
						.format("Matching %,d x %,d elements; %,d blocked pairs (reduction ratio: %s)",
								schema1.getSize(), schema2.getSize(),
								allPairs.size(), Double.toString(blocker.getReductionRatio())));

		// compare the pairs using the matching rule
		ProgressReporter progress = new ProgressReporter(allPairs.size(),
				"Schema Matching");
		for(BlockedMatchable<SchemaElementType, RecordType> task : allPairs.get()) {

			// apply the matching rule
			Correspondence<SchemaElementType, RecordType> cor = rule.apply(task.getFirstRecord(), task.getSecondRecord(), task.getSchemaCorrespondences());
			if (cor != null) {

				// add the correspondences to the result
				result.add(cor);
			}

			// increment and report status
			progress.incrementProgress();
			progress.report();
		}

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
	public ResultSet<Correspondence<SchemaElementType, RecordType>> runSchemaMatching(
			DataSet<SchemaElementType, SchemaElementType> schema1, 
			DataSet<SchemaElementType, SchemaElementType> schema2,
			ResultSet<Correspondence<RecordType, SchemaElementType>> instanceCorrespondences,
			SchemaMatchingRule<RecordType, SchemaElementType, SchemaElementType> rule) {
		long start = System.currentTimeMillis();

		System.out.println(String.format("[%s] Starting Schema Matching",
				new DateTime(start).toString()));

		ResultSet<Correspondence<SchemaElementType, RecordType>> result = new ResultSet<>();

		System.out
				.println(String
						.format("Matching %,d elements",
								schema1.size() + schema2.size()));

		// compare the pairs using the matching rule
		ProgressReporter progress = new ProgressReporter(instanceCorrespondences.size(),
				"Schema Matching");
		for (Correspondence<RecordType, SchemaElementType> correspondence : instanceCorrespondences.get()) {

			// apply the matching rule
			ResultSet<Correspondence<SchemaElementType, RecordType>> cor = rule.apply(schema1, schema2, correspondence);
			if (cor != null) {

				// add the correspondences to the result
				result.merge(cor);
			}

			// increment and report status
			progress.incrementProgress();
			progress.report();
		}

		// report total matching time
		long end = System.currentTimeMillis();
		long delta = end - start;
		System.out.println(String.format(
				"[%s] Matching finished after %s; found %d correspondences.",
				new DateTime(end).toString(),
				DurationFormatUtils.formatDurationHMS(delta), result.size()));

		return result;
	}
	
	public ResultSet<Correspondence<SchemaElementType, RecordType>> runDuplicateBasedSchemaMatching(
			DataSet<SchemaElementType, SchemaElementType> schema1, 
			DataSet<SchemaElementType, SchemaElementType> schema2,
			ResultSet<Correspondence<RecordType, SchemaElementType>> instanceCorrespondences,
			SchemaMatchingRuleWithVoting<RecordType, SchemaElementType, SchemaElementType> rule) {
		return runDuplicateBasedSchemaMatching(schema1, schema2, instanceCorrespondences, rule, new StandardSchemaBlocker<SchemaElementType, RecordType>(new StaticBlockingKeyGenerator<SchemaElementType>()));
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
	public ResultSet<Correspondence<SchemaElementType, RecordType>> runDuplicateBasedSchemaMatching(
			DataSet<SchemaElementType, SchemaElementType> schema1, 
			DataSet<SchemaElementType, SchemaElementType> schema2,
			ResultSet<Correspondence<RecordType, SchemaElementType>> instanceCorrespondences,
			SchemaMatchingRuleWithVoting<RecordType, SchemaElementType, SchemaElementType> rule,
			SchemaBlocker<SchemaElementType, RecordType> schemaBlocker) {
		long start = System.currentTimeMillis();

		System.out.println(String.format("[%s] Starting Duplicate-based Schema Matching",
				new DateTime(start).toString()));

		ResultSet<Pair<Correspondence<RecordType, SchemaElementType>,Correspondence<SchemaElementType, RecordType>>> result = new ResultSet<>();

		System.out.println(String.format("Blocking %,d x %,d elements", schema1.getSize(), schema2.getSize()));
		
		ResultSet<BlockedMatchable<SchemaElementType, RecordType>> blocked = schemaBlocker.runBlocking(schema1, schema2, instanceCorrespondences, getProcessingEngine());
		
		System.out
		.println(String
				.format("Matching %,d x %,d elements; %,d blocked pairs (reduction ratio: %s)",
						schema1.getSize(), schema2.getSize(),
						blocked.size(), Double.toString(schemaBlocker.getReductionRatio())));

		int columnCombinations = blocked.size();
		// compare the pairs using the matching rule
		ProgressReporter progress = new ProgressReporter(columnCombinations,
				"Duplicate-based Schema Matching");
//		for (Correspondence<RecordType, SchemaElementType> correspondence : instanceCorrespondences.get()) {
		for(BlockedMatchable<SchemaElementType, RecordType> blockedMatchable : blocked.get()) {
			
			for (Correspondence<RecordType, SchemaElementType> correspondence : blockedMatchable.getSchemaCorrespondences().get()) {


				// apply the matching rule
				Correspondence<SchemaElementType, RecordType> c = rule.apply(blockedMatchable.getFirstRecord(), blockedMatchable.getSecondRecord(), correspondence);
				
				if (c != null) {
					Pair<Correspondence<RecordType, SchemaElementType>,Correspondence<SchemaElementType, RecordType>> cor = new Pair<>(correspondence, c);
					
					// add the correspondences to the result
					result.add(cor);
				}

			}
			
			// increment and report status
			progress.incrementProgress();
			progress.report();
		}

		// aggregate results: voting
		ResultSet<Correspondence<SchemaElementType, RecordType>> finalResult = rule.aggregate(result, instanceCorrespondences.size(), getProcessingEngine());
		
		// report total matching time
		long end = System.currentTimeMillis();
		long delta = end - start;
		System.out.println(String.format(
				"[%s] Duplicate-based Schema Matching finished after %s; found %d correspondences from %,d votes.",
				new DateTime(end).toString(),
				DurationFormatUtils.formatDurationHMS(delta), finalResult.size(), result.size()));

		return finalResult;
	}
	
//	public void runIteration(DataSet<RecordType, SchemaElementType> dataset, DatasetIterator<RecordType> iterator) {
//		processingEngine.iterateDataset(dataset, iterator);
//	}
//	
//	public <KeyType> ResultSet<Group<KeyType, RecordType>> runGrouping(DataSet<RecordType, SchemaElementType> dataset, RecordMapper<KeyType, RecordType> groupBy) {
//		return processingEngine.groupRecords(dataset, groupBy);
//	}
//	
//	public <KeyType, ResultType> ResultSet<Pair<KeyType, ResultType>> aggregateGroups(ResultSet<Group<KeyType, RecordType>> groups, DataAggregator<RecordType, ResultType> aggregator) {
//		return processingEngine.aggregateGroups(groups, aggregator);
//	}
	
	/**
	 * @return the processingEngine
	 */
	public DataProcessingEngine getProcessingEngine() {
		return processingEngine;
	}
}
