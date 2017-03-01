package de.uni_mannheim.informatik.wdi.matching.blocking;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import junit.framework.TestCase;

import org.xml.sax.SAXException;

import de.uni_mannheim.informatik.wdi.matching.MatchingEngine;
import de.uni_mannheim.informatik.wdi.matching.MatchingEvaluator;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.DefaultDataSet;
import de.uni_mannheim.informatik.wdi.model.DefaultSchemaElement;
import de.uni_mannheim.informatik.wdi.model.MatchingGoldStandard;
import de.uni_mannheim.informatik.wdi.model.Performance;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.usecase.movies.model.Movie;
import de.uni_mannheim.informatik.wdi.usecase.movies.model.MovieFactory;

public class StandardBlockerTest extends TestCase {

	public void testGeneratePairs() throws XPathExpressionException,
			ParserConfigurationException, SAXException, IOException {
		DefaultDataSet<Movie, DefaultSchemaElement> ds = new DefaultDataSet<>();
		File sourceFile1 = new File("usecase/movie/input/actors.xml");
		ds.loadFromXML(sourceFile1, new MovieFactory(), "/movies/movie");

		DefaultDataSet<Movie, DefaultSchemaElement> ds2 = new DefaultDataSet<>();
		File sourceFile2 = new File("usecase/movie/input/academy_awards.xml");
		ds2.loadFromXML(sourceFile2, new MovieFactory(), "/movies/movie");

		Blocker<Movie, DefaultSchemaElement> blocker = new StandardBlocker<>(
				new StaticBlockingKeyGenerator<Movie>());

		MatchingGoldStandard gs = new MatchingGoldStandard();
		gs.loadFromCSVFile(new File(
				"usecase/movie/goldstandard/gs_academy_awards_2_actors.csv"));

		MatchingEngine<Movie, DefaultSchemaElement> engine = new MatchingEngine<>();
		
		ResultSet<BlockedMatchable<Movie, DefaultSchemaElement>> pairs = blocker.runBlocking(ds, ds2, null, engine.getProcessingEngine());
		
		List<Correspondence<Movie, DefaultSchemaElement>> correspondences = new ArrayList<>(
				pairs.size());

		// transform pairs into correspondences
		for (BlockedMatchable<Movie, DefaultSchemaElement> p : pairs.get()) {
			correspondences.add(new Correspondence<Movie, DefaultSchemaElement>(p.getFirstRecord(), p
					.getSecondRecord(), 1.0, p.getSchemaCorrespondences()));
		}

		// check if all examples from the gold standard were in the pairs
		MatchingEvaluator<Movie, DefaultSchemaElement> eval = new MatchingEvaluator<>(true);

		Performance perf = eval.evaluateMatching(correspondences, gs);

		assertEquals(1.0, perf.getRecall());
	}

}
