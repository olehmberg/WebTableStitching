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
import de.uni_mannheim.informatik.wdi.matching.blocking.Blocker;
import de.uni_mannheim.informatik.wdi.matching.blocking.StandardBlocker;
import de.uni_mannheim.informatik.wdi.matching.blocking.StaticBlockingKeyGenerator;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.DefaultDataSet;
import de.uni_mannheim.informatik.wdi.model.DefaultSchemaElement;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.usecase.movies.identityresolution.MovieDateComparator10Years;
import de.uni_mannheim.informatik.wdi.usecase.movies.identityresolution.MovieTitleComparatorLevenshtein;
import de.uni_mannheim.informatik.wdi.usecase.movies.model.Movie;
import de.uni_mannheim.informatik.wdi.usecase.movies.model.MovieFactory;

/**
 * Class containing the standard setup to perform a duplicate detection task,
 * reading input data from the movie usecase.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 * @author Robert Meusel (robert@dwslab.de)
 * 
 */
public class Movies_DuplicateDetection_Main {

	public static void main(String[] args) throws Exception {

		// define the matching rule
		LinearCombinationMatchingRule<Movie, DefaultSchemaElement> rule = new LinearCombinationMatchingRule<>(
				0, 1.0);
		rule.addComparator(new MovieTitleComparatorLevenshtein(), 1);
		rule.addComparator(new MovieDateComparator10Years(), 0.822);

		// create the matching engine
		Blocker<Movie, DefaultSchemaElement> blocker = new StandardBlocker<Movie, DefaultSchemaElement>(
				new StaticBlockingKeyGenerator<Movie>());
		MatchingEngine<Movie, DefaultSchemaElement> engine = new MatchingEngine<>();

		// load the data sets
		DefaultDataSet<Movie, DefaultSchemaElement> ds1 = new DefaultDataSet<>();
		ds1.loadFromXML(new File("usecase/movie/input/actors.xml"),
				new MovieFactory(), "/movies/movie");

		// run the matching
		ResultSet<Correspondence<Movie, DefaultSchemaElement>> correspondences = engine
				.runDuplicateDetection(ds1, true, null, rule, blocker);

		// write the correspondences to the output file
		engine.writeCorrespondences(correspondences.get(), new File(
				"usecase/movie/output/actors_duplicates.csv"));

		printCorrespondences(new ArrayList<>(correspondences.get()));
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
			// if(correspondence.getSimilarityScore()<1.0) {
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
			// }
		}
	}

}
