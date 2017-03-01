package de.uni_mannheim.informatik.wdi.matching;

import java.util.LinkedList;

import de.uni_mannheim.informatik.wdi.matching.MatchingEvaluator;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.DefaultSchemaElement;
import de.uni_mannheim.informatik.wdi.model.MatchingGoldStandard;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.Performance;

import java.util.List;

import de.uni_mannheim.informatik.wdi.usecase.movies.model.Movie;
import junit.framework.TestCase;

public class MatchingEvaluatorTest extends TestCase {

	public void testEvaluateMatching() {
		MatchingEvaluator<Movie, DefaultSchemaElement> evaluator = new MatchingEvaluator<>();
		List<Correspondence<Movie, DefaultSchemaElement>> correspondences = new LinkedList<>();
		MatchingGoldStandard gold = new MatchingGoldStandard();
		
		Movie movie1 = new Movie("movie1", "test");
		Movie movie2 = new Movie("movie2", "test");
		Movie movie3 = new Movie("movie3", "test2");
		Movie movie4 = new Movie("movie4", "test2");

		correspondences.add(new Correspondence<Movie, DefaultSchemaElement>(movie1, movie3, 1.0, null));
		correspondences.add(new Correspondence<Movie, DefaultSchemaElement>(movie1, movie2, 1.0, null));
		
		gold.addPositiveExample(new Pair<String, String>(movie3.getIdentifier(), movie1.getIdentifier()));
		gold.addPositiveExample(new Pair<String, String>(movie2.getIdentifier(), movie4.getIdentifier()));
		gold.addNegativeExample(new Pair<String, String>(movie1.getIdentifier(), movie2.getIdentifier()));
		gold.addNegativeExample(new Pair<String, String>(movie3.getIdentifier(), movie4.getIdentifier()));
		
		Performance p = evaluator.evaluateMatching(correspondences, gold);
		
		assertEquals(0.5, p.getPrecision());
		assertEquals(0.5, p.getRecall());
	}

}
