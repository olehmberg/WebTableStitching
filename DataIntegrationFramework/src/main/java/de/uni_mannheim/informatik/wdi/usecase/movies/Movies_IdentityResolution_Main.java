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
package de.uni_mannheim.informatik.wdi.usecase.movies;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import de.uni_mannheim.informatik.wdi.matching.LinearCombinationMatchingRule;
import de.uni_mannheim.informatik.wdi.matching.MatchingEngine;
import de.uni_mannheim.informatik.wdi.matching.blocking.NoBlocker;
import de.uni_mannheim.informatik.wdi.matching.blocking.StandardBlocker;
import de.uni_mannheim.informatik.wdi.matching.blocking.StaticBlockingKeyGenerator;
import de.uni_mannheim.informatik.wdi.matching.MatchingEvaluator;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.DefaultDataSet;
import de.uni_mannheim.informatik.wdi.model.DefaultRecordCSVFormatter;
import de.uni_mannheim.informatik.wdi.model.DefaultSchemaElement;
import de.uni_mannheim.informatik.wdi.model.FeatureVectorDataSet;
import de.uni_mannheim.informatik.wdi.model.MatchingGoldStandard;
import de.uni_mannheim.informatik.wdi.model.Performance;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.usecase.movies.identityresolution.MovieBlockingKeyByYearGenerator;
import de.uni_mannheim.informatik.wdi.usecase.movies.identityresolution.MovieDateComparator10Years;
import de.uni_mannheim.informatik.wdi.usecase.movies.identityresolution.MovieTitleComparatorEqual;
import de.uni_mannheim.informatik.wdi.usecase.movies.identityresolution.MovieTitleComparatorJaccard;
import de.uni_mannheim.informatik.wdi.usecase.movies.identityresolution.MovieTitleComparatorLevenshtein;
import de.uni_mannheim.informatik.wdi.usecase.movies.model.Movie;
import de.uni_mannheim.informatik.wdi.usecase.movies.model.MovieFactory;

/**
 * Class containing the standard setup to perform a identity resolution task,
 * reading input data from the movie usecase.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 * @author Robert Meusel (robert@dwslab.de)
 * 
 */
public class Movies_IdentityResolution_Main {

	public static void main(String[] args) throws Exception {
		// loading data
		DefaultDataSet<Movie, DefaultSchemaElement> dataAcademyAwards = new DefaultDataSet<>();
		dataAcademyAwards.loadFromXML(new File(
				"usecase/movie/input/academy_awards.xml"), new MovieFactory(),
				"/movies/movie");
		DefaultDataSet<Movie, DefaultSchemaElement> dataActors = new DefaultDataSet<>();
		dataActors.loadFromXML(new File("usecase/movie/input/actors.xml"),
				new MovieFactory(), "/movies/movie");

		// create a matching rule
		LinearCombinationMatchingRule<Movie, DefaultSchemaElement> matchingRule = new LinearCombinationMatchingRule<>(
				0.7);
		// add comparators
		matchingRule.addComparator(new MovieTitleComparatorJaccard(), 0.8);
		matchingRule.addComparator(new MovieDateComparator10Years(), 0.2);

		// create a blocker (blocking strategy)
		NoBlocker<Movie, DefaultSchemaElement> blocker = new NoBlocker<>();

		// Initialize Matching Engine
		MatchingEngine<Movie, DefaultSchemaElement> engine = new MatchingEngine<>();

		// Execute the matching
		ResultSet<Correspondence<Movie, DefaultSchemaElement>> correspondences = engine.runIdentityResolution(
				dataAcademyAwards, dataActors, null, matchingRule,
				blocker);

		// write the correspondences to the output file
		engine.writeCorrespondences(
				correspondences.get(),
				new File(
						"usecase/movie/output/academy_awards_2_actors_correspondences.csv"));

		// print the correspondences to console
		// printCorrespondences(correspondences);

		// load the gold standard (test set)
		MatchingGoldStandard gsTest = new MatchingGoldStandard();
		gsTest.loadFromCSVFile(new File(
				"usecase/movie/goldstandard/gs_academy_awards_2_actors_v2.csv"));

		// evaluate your result
		MatchingEvaluator<Movie, DefaultSchemaElement> evaluator = new MatchingEvaluator<Movie, DefaultSchemaElement>(true);
		Performance perfTest = evaluator.evaluateMatching(correspondences.get(),
				gsTest);

		// print the evaluation result
		System.out.println("Academy Awards <-> Actors");
		System.out
				.println(String.format(
						"Precision: %.4f\nRecall: %.4f\nF1: %.4f",
						perfTest.getPrecision(), perfTest.getRecall(),
						perfTest.getF1()));
	}

	public static void createDatasetToTrain() throws Exception {
		// loading data
		DefaultDataSet<Movie, DefaultSchemaElement> dataAcademyAwards = new DefaultDataSet<>();
		dataAcademyAwards.loadFromXML(new File(
				"usecase/movie/input/academy_awards.xml"), new MovieFactory(),
				"/movies/movie");
		DefaultDataSet<Movie, DefaultSchemaElement> dataActors = new DefaultDataSet<>();
		dataActors.loadFromXML(new File("usecase/movie/input/actors.xml"),
				new MovieFactory(), "/movies/movie");

		// load the gold standard (test set)
		// load the gold standard (training set)
		MatchingGoldStandard gsTraining = new MatchingGoldStandard();
		gsTraining.loadFromCSVFile(new File(
				"usecase/movie/goldstandard/gs_academy_awards_2_actors.csv"));

		// create a matching rule
		LinearCombinationMatchingRule<Movie, DefaultSchemaElement> matchingRule = new LinearCombinationMatchingRule<>(
				0.0);
		// add comparators
		matchingRule.addComparator(new MovieTitleComparatorLevenshtein(), 0.5);
		matchingRule.addComparator(new MovieDateComparator10Years(), 0.5);

		// Initialize Matching Engine
		MatchingEngine<Movie, DefaultSchemaElement> engine = new MatchingEngine<>();

		// create the data set for learning a matching rule (use this file in
		// RapidMiner)
		FeatureVectorDataSet features = new FeatureVectorDataSet();
		engine.generateTrainingDataForLearning(dataAcademyAwards, dataActors,
						gsTraining, matchingRule, null, features);
		features.writeCSV(
				new File(
						"usecase/movie/output/optimisation/academy_awards_2_actors_features.csv"),
				new DefaultRecordCSVFormatter());
	}

	public static void firstMatching() throws Exception {

		// loading data
		DefaultDataSet<Movie, DefaultSchemaElement> dataAcademyAwards = new DefaultDataSet<>();
		dataAcademyAwards.loadFromXML(new File(
				"usecase/movie/input/academy_awards.xml"), new MovieFactory(),
				"/movies/movie");
		DefaultDataSet<Movie, DefaultSchemaElement> dataActors = new DefaultDataSet<>();
		dataActors.loadFromXML(new File("usecase/movie/input/actors.xml"),
				new MovieFactory(), "/movies/movie");

		// create a matching rule
		LinearCombinationMatchingRule<Movie, DefaultSchemaElement> matchingRule = new LinearCombinationMatchingRule<>(
				0.0);
		// add comparators
		matchingRule.addComparator(new MovieTitleComparatorEqual(), 1);
		matchingRule.addComparator(new MovieDateComparator10Years(), 1);
		// run normalization
		matchingRule.normalizeWeights();

		// create a blocker (blocking strategy)
		StandardBlocker<Movie, DefaultSchemaElement> blocker = new StandardBlocker<>(
				new StaticBlockingKeyGenerator<Movie>());

		// Initialize Matching Engine
		MatchingEngine<Movie, DefaultSchemaElement> engine = new MatchingEngine<>();

		// Execute the matching
		ResultSet<Correspondence<Movie, DefaultSchemaElement>> correspondences = engine.runIdentityResolution(
				dataAcademyAwards, dataActors, null, matchingRule,
				blocker);

		// write the correspondences to the output file
		engine.writeCorrespondences(
				correspondences.get(),
				new File(
						"usecase/movie/output/academy_awards_2_actors_correspondences.csv"));

		// print the correspondences to console
		// printCorrespondences(correspondences);

		// load the gold standard (test set)
		MatchingGoldStandard gsTest = new MatchingGoldStandard();
		gsTest.loadFromCSVFile(new File(
				"usecase/movie/goldstandard/gs_academy_awards_2_actors_test.csv"));

		// evaluate your result
		MatchingEvaluator<Movie, DefaultSchemaElement> evaluator = new MatchingEvaluator<Movie, DefaultSchemaElement>(true);
		Performance perfTest = evaluator.evaluateMatching(correspondences.get(),
				gsTest);

		// print the evaluation result
		System.out.println("Academy Awards <-> Actors");
		System.out
				.println(String.format(
						"Precision: %.4f\nRecall: %.4f\nF1: %.4f",
						perfTest.getPrecision(), perfTest.getRecall(),
						perfTest.getF1()));
	}

	public static void runWhole() throws Exception {
		// define the matching rule
		LinearCombinationMatchingRule<Movie, DefaultSchemaElement> rule = new LinearCombinationMatchingRule<>(
				-1.497, 0.5);
		// LinearCombinationMatchingRule<Movie> rule = new
		// LinearCombinationMatchingRule<>(0.0);
		rule.addComparator(new MovieTitleComparatorLevenshtein(), 1.849);
		rule.addComparator(new MovieDateComparator10Years(), 0.822);

		// create the matching engine
		StandardBlocker<Movie, DefaultSchemaElement> blocker = new StandardBlocker<>(
				new MovieBlockingKeyByYearGenerator());
		MatchingEngine<Movie, DefaultSchemaElement> engine = new MatchingEngine<>();

		// load the data sets
		DefaultDataSet<Movie, DefaultSchemaElement> ds1 = new DefaultDataSet<>();
		DefaultDataSet<Movie, DefaultSchemaElement> ds2 = new DefaultDataSet<>();
		DefaultDataSet<Movie, DefaultSchemaElement> ds3 = new DefaultDataSet<>();
		ds1.loadFromXML(new File("usecase/movie/input/academy_awards.xml"),
				new MovieFactory(), "/movies/movie");
		ds2.loadFromXML(new File("usecase/movie/input/actors.xml"),
				new MovieFactory(), "/movies/movie");
		ds3.loadFromXML(new File("usecase/movie/input/golden_globes.xml"),
				new MovieFactory(), "/movies/movie");

		// run the matching
		ResultSet<Correspondence<Movie, DefaultSchemaElement>> correspondences = engine.runIdentityResolution(ds1,
				ds2, null, rule, blocker);
		ResultSet<Correspondence<Movie, DefaultSchemaElement>> correspondences2 = engine.runIdentityResolution(ds2,
				ds3, null, rule, blocker);

		// write the correspondences to the output file
		engine.writeCorrespondences(
				correspondences.get(),
				new File(
						"usecase/movie/output/academy_awards_2_actors_correspondences.csv"));
		engine.writeCorrespondences(
				correspondences2.get(),
				new File(
						"usecase/movie/output/actors_2_golden_globes_correspondences.csv"));

		printCorrespondences(new ArrayList<>(correspondences2.get()));

		// load the gold standard (training set)
		MatchingGoldStandard gsTraining = new MatchingGoldStandard();
		gsTraining.loadFromCSVFile(new File(
				"usecase/movie/goldstandard/gs_academy_awards_2_actors.csv"));

		// create the data set for learning a matching rule (use this file in
		// RapidMiner)
		FeatureVectorDataSet features = new FeatureVectorDataSet();
		engine.generateTrainingDataForLearning(ds1, ds2, gsTraining, rule, null, features);
		features.writeCSV(
				new File(
						"usecase/movie/output/optimisation/academy_awards_2_actors_features.csv"),
				new DefaultRecordCSVFormatter());

		// load the gold standard (test set)
		MatchingGoldStandard gsTest = new MatchingGoldStandard();
		gsTest.loadFromCSVFile(new File(
				"usecase/movie/goldstandard/gs_academy_awards_2_actors_test.csv"));
		MatchingGoldStandard gs2 = new MatchingGoldStandard();
		gs2.loadFromCSVFile(new File(
				"usecase/movie/goldstandard/gs_actors_2_golden_globes.csv"));

		// evaluate the result
		MatchingEvaluator<Movie, DefaultSchemaElement> evaluator = new MatchingEvaluator<>(true);
		Performance perfTest = evaluator.evaluateMatching(correspondences.get(),
				gsTest);
		Performance perf2 = evaluator.evaluateMatching(correspondences2.get(), gs2);

		// print the evaluation result
		System.out.println("Academy Awards <-> Actors");
		System.out
				.println(String.format(
						"Precision: %.4f\nRecall: %.4f\nF1: %.4f",
						perfTest.getPrecision(), perfTest.getRecall(),
						perfTest.getF1()));

		System.out.println("Actors <-> Golden Globes");
		System.out.println(String.format(
				"Precision: %.4f\nRecall: %.4f\nF1: %.4f",
				perf2.getPrecision(), perf2.getRecall(), perf2.getF1()));
	}

	private static void printCorrespondences(
			List<Correspondence<Movie, DefaultSchemaElement>> correspondences) {
		// sort the correspondences
		Collections.sort(correspondences,
				new Comparator<Correspondence<Movie, DefaultSchemaElement>>() {

					@Override
					public int compare(Correspondence<Movie, DefaultSchemaElement> o1,
							Correspondence<Movie, DefaultSchemaElement> o2) {
						int score = Double.compare(o1.getSimilarityScore(),
								o2.getSimilarityScore());
						int title = o1.getFirstRecord().getTitle()
								.compareTo(o2.getFirstRecord().getTitle());

						if (score != 0) {
							return -score;
						} else {
							return title;
						}
					}

				});

		// print the correspondences
		for (Correspondence<Movie, DefaultSchemaElement> correspondence : correspondences) {
			System.out.println(String
					.format("%s,%s,|\t\t%.2f\t[%s] %s (%s) <--> [%s] %s (%s)",
							correspondence.getFirstRecord().getIdentifier(),
							correspondence.getSecondRecord().getIdentifier(),
							correspondence.getSimilarityScore(),
							correspondence.getFirstRecord().getIdentifier(),
							correspondence.getFirstRecord().getTitle(),
							correspondence.getFirstRecord().getDate()
									.toString("YYYY-MM-DD"), correspondence
									.getSecondRecord().getIdentifier(),
							correspondence.getSecondRecord().getTitle(),
							correspondence.getSecondRecord().getDate()
									.toString("YYYY-MM-DD")));
		}
	}

}
