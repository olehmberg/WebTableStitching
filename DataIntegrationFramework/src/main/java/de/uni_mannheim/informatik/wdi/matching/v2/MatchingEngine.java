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
package de.uni_mannheim.informatik.wdi.matching.v2;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.joda.time.DateTime;

import au.com.bytecode.opencsv.CSVWriter;

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
import de.uni_mannheim.informatik.wdi.processing.DataAggregator;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;
import de.uni_mannheim.informatik.wdi.similarity.string.LevenshteinSimilarity;
import de.uni_mannheim.informatik.wdi.similarity.string.TokenizingJaccardSimilarity;
import de.uni_mannheim.informatik.wdi.utils.ProgressReporter;

/**
 * 
 * Facade for generic matchers with pre-implemented algorithms.
 * 
 * Datasets with <RecordType, SchemaElementType> are a row-oriented representation of the data.
 * Datasets with <SchemaElementType, RecordType> are a column-oriented representation of the data.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class MatchingEngine  {
	
	public MatchingEngine() {
		this.processingEngine = new DataProcessingEngine();
	}
	
	public MatchingEngine(DataProcessingEngine processingEngine) {
		this.processingEngine = processingEngine;
	}
	
	protected DataProcessingEngine processingEngine;
	
	/**
	 * @return the processingEngine
	 */
	public DataProcessingEngine getProcessingEngine() {
		return processingEngine;
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
	public <RecordType extends Matchable, SchemaElementType extends Matchable> ResultSet<Correspondence<RecordType, SchemaElementType>> runDuplicateDetection(
			DataSet<RecordType, SchemaElementType> dataset, 
			boolean isSymmetric, 
			ResultSet<Correspondence<SchemaElementType, RecordType>> typeBCorrespondences,
			MatchingRule<RecordType, SchemaElementType> rule,
			Blocker<RecordType, SchemaElementType> blocker) {
		long start = System.currentTimeMillis();

		System.out.println(String.format("[%s] Starting Duplicate Detection",
				new DateTime(start).toString()));

		ResultSet<Correspondence<RecordType, SchemaElementType>> blocked = blocker.runBlocking(dataset, isSymmetric, typeBCorrespondences, getProcessingEngine());
		
		System.out
		.println(String
				.format("Duplicate Detection %,d x %,d elements; %,d blocked pairs (reduction ratio: %.2f)",
						dataset.getSize(), dataset.getSize(),
						blocked.size(), blocker.getReductionRatio()));
		
		Matcher<RecordType, SchemaElementType> matcher = new Matcher<>(getProcessingEngine(), rule);
		
		ResultSet<Correspondence<RecordType, SchemaElementType>> matched = matcher.runMatching(blocked);

		// report total Duplicate Detection time
		long end = System.currentTimeMillis();
		long delta = end - start;
		System.out
				.println(String
						.format("[%s] Duplicate Detection finished after %s; found %,d correspondences.",
								new DateTime(end).toString(),
								DurationFormatUtils.formatDurationHMS(delta),
								matched.size()));

		return matched;
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
	public <RecordType extends Matchable, SchemaElementType extends Matchable> ResultSet<Correspondence<RecordType, SchemaElementType>> runIdentityResolution(
			DataSet<RecordType, SchemaElementType> dataset1, 
			DataSet<RecordType, SchemaElementType> dataset2, 
			ResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences,
			MatchingRule<RecordType, SchemaElementType> rule,
			Blocker<RecordType, SchemaElementType> blocker) {
		long start = System.currentTimeMillis();

		System.out.println(String.format("[%s] Starting Identity Resolution",
				new DateTime(start).toString()));

		ResultSet<Correspondence<RecordType, SchemaElementType>> blocked = blocker.runBlocking(dataset1, dataset2, schemaCorrespondences, getProcessingEngine());

		System.out
				.println(String
						.format("Matching %,d x %,d elements; %,d blocked pairs (reduction ratio: %s)",
								dataset1.getSize(), dataset2.getSize(),
								blocked.size(), Double.toString(blocker.getReductionRatio())));

		Matcher<RecordType, SchemaElementType> matcher = new Matcher<>(getProcessingEngine(), rule);
		
		ResultSet<Correspondence<RecordType, SchemaElementType>> matched = matcher.runMatching(blocked);		

		// report total matching time
		long end = System.currentTimeMillis();
		long delta = end - start;
		System.out.println(String.format(
				"[%s] Identity Resolution finished after %s; found %,d correspondences.",
				new DateTime(end).toString(),
				DurationFormatUtils.formatDurationHMS(delta), matched.size()));

		return matched;
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
	public <RecordType extends Matchable, SchemaElementType extends Matchable> void generateTrainingDataForLearning(
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

	//TODO why does the matching engine implement the writing?
	public <RecordType extends Matchable, SchemaElementType extends Matchable> void writeCorrespondences(
			Collection<Correspondence<RecordType, SchemaElementType>> correspondences, File file)
			throws IOException {
		CSVWriter w = new CSVWriter(new FileWriter(file));

		for (Correspondence<RecordType, SchemaElementType> c : correspondences) {
			w.writeNext(new String[] { c.getFirstRecord().getIdentifier(),
					c.getSecondRecord().getIdentifier(),
					Double.toString(c.getSimilarityScore()) });
		}

		w.close();
	}

	public <RecordType extends Matchable, SchemaElementType extends Matchable> ResultSet<Correspondence<SchemaElementType, RecordType>> runSchemaMatching(
			DataSet<SchemaElementType, RecordType> schema1, 
			DataSet<SchemaElementType, RecordType> schema2,
			ResultSet<Correspondence<RecordType, SchemaElementType>> instanceCorrespondences,
			MatchingRule<SchemaElementType, RecordType> rule,
			Blocker<SchemaElementType, RecordType> blocker) {
		long start = System.currentTimeMillis();

		System.out.println(String.format("[%s] Starting Schema Matching",
				new DateTime(start).toString()));

		ResultSet<Correspondence<SchemaElementType, RecordType>> blocked = blocker.runBlocking(schema1, schema2, instanceCorrespondences, getProcessingEngine());

		System.out
				.println(String
						.format("Matching %,d x %,d elements; %,d blocked pairs (reduction ratio: %s)",
								schema1.getSize(), schema2.getSize(),
								blocked.size(), Double.toString(blocker.getReductionRatio())));

		Matcher<SchemaElementType, RecordType> matcher = new Matcher<>(getProcessingEngine(), rule);
		
		ResultSet<Correspondence<SchemaElementType, RecordType>> matched = matcher.runMatching(blocked);

		// report total matching time
		long end = System.currentTimeMillis();
		long delta = end - start;
		System.out.println(String.format(
				"[%s] Identity Resolution finished after %s; found %,d correspondences.",
				new DateTime(end).toString(),
				DurationFormatUtils.formatDurationHMS(delta), matched.size()));

		return matched;
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
	public <RecordType extends Matchable, SchemaElementType extends Matchable> ResultSet<Correspondence<SchemaElementType, RecordType>> runSchemaMatching(
			DataSet<SchemaElementType, RecordType> schema1, 
			DataSet<SchemaElementType, RecordType> schema2,
			ResultSet<Correspondence<RecordType, SchemaElementType>> instanceCorrespondences,
			Blocker<SchemaElementType, RecordType> blocker) {
		long start = System.currentTimeMillis();

		System.out.println(String.format("[%s] Starting Schema Matching",
				new DateTime(start).toString()));

		System.out
				.println(String
						.format("Matching %,d elements",
								schema1.size() + schema2.size()));

		ResultSet<Correspondence<SchemaElementType, RecordType>> blocked = blocker.runBlocking(schema1, schema2, instanceCorrespondences, getProcessingEngine());

		// report total matching time
		long end = System.currentTimeMillis();
		long delta = end - start;
		System.out.println(String.format(
				"[%s] Matching finished after %s; found %d correspondences.",
				new DateTime(end).toString(),
				DurationFormatUtils.formatDurationHMS(delta), blocked.size()));

		return blocked;
	}
	
	/**
	 * Runs duplicate-based schema matching.
	 * Expects column-oriented datasets (each record of the dataset is a SchemaElement).
	 * 
	 * @param schema1
	 *            The first schema
	 * @param schema2
	 *            The second schema
	 * @param instanceCorrespondences
	 * 			  The instance correspondences that represent the duplicate records in both schemas
	 * @param blocker
	 * 			  A blocker that creates all desired combinations of schema elements for every duplicate record.
	 * 			  Should return one correspondence for each SchemaElement-SchemaElement-Record-Record combination, reflecting exactly two values which will be compared
	 * @param rule
	 * 			  A matching rule that compares two schema elements based on a duplicate record in both schemas.
	 * 			  Should return one correspondence for each vote that is cast by the duplicate record
	 * @param voting
	 * 			  An data aggregator that turns the votes into the final correspondences
	 * @return A list of correspondences
	 */
	public <RecordType extends Matchable, SchemaElementType extends Matchable> ResultSet<Correspondence<SchemaElementType, RecordType>> runDuplicateBasedSchemaMatching(
			DataSet<SchemaElementType, RecordType> schema1, 
			DataSet<SchemaElementType, RecordType> schema2,
			ResultSet<Correspondence<RecordType, SchemaElementType>> instanceCorrespondences,
			Blocker<SchemaElementType, RecordType> blocker,
			MatchingRule<SchemaElementType, RecordType> rule,
			DataAggregator<Correspondence<SchemaElementType, RecordType>, Correspondence<SchemaElementType, RecordType>,Correspondence<SchemaElementType, RecordType>> voting) {
		long start = System.currentTimeMillis();

		System.out.println(String.format("[%s] Starting Duplicate-based Schema Matching",
				new DateTime(start).toString()));

		// creates one correspondence for every SchemaElement-SchemaElement-Record combination, so one for each potential vote
		ResultSet<Correspondence<SchemaElementType, RecordType>> blocked = blocker.runBlocking(schema1, schema2, instanceCorrespondences, getProcessingEngine());

		System.out
				.println(String
						.format("Matching %,d elements",
								schema1.size() + schema2.size()));

		Matcher<SchemaElementType, RecordType> matcher = new Matcher<>(getProcessingEngine(), rule);
		
		ResultSet<Correspondence<SchemaElementType, RecordType>> matched = matcher.runMatching(blocked);

		Aggregator<SchemaElementType, RecordType> aggregator = new Aggregator<>(getProcessingEngine());
		
		ResultSet<Correspondence<SchemaElementType, RecordType>> aggregated = aggregator.runAggregation(matched, voting);
		
		// report total matching time
		long end = System.currentTimeMillis();
		long delta = end - start;
		System.out.println(String.format(
				"[%s] Duplicate-based Schema Matching finished after %s; found %d correspondences from %,d votes.",
				new DateTime(end).toString(),
				DurationFormatUtils.formatDurationHMS(delta), aggregated.size(), matched.size()));

		return aggregated;
	}

}
