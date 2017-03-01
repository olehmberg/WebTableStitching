package de.uni_mannheim.informatik.wdi.matching.blocking;

import java.io.File;
import java.io.IOException;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import junit.framework.TestCase;

import org.joda.time.DateTime;
import org.xml.sax.SAXException;

import de.uni_mannheim.informatik.wdi.matching.MatchingEngine;
import de.uni_mannheim.informatik.wdi.model.DataSet;
import de.uni_mannheim.informatik.wdi.model.DefaultDataSet;
import de.uni_mannheim.informatik.wdi.model.DefaultSchemaElement;
import de.uni_mannheim.informatik.wdi.model.MatchingGoldStandard;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.usecase.movies.identityresolution.MovieBlockingKeyByYearGenerator;
import de.uni_mannheim.informatik.wdi.usecase.movies.model.Movie;

public class SortedNeighbourhoodBlockerTest extends TestCase {

	private DataSet<Movie, DefaultSchemaElement> generateDS1() {
		DataSet<Movie, DefaultSchemaElement> ds = new DefaultDataSet<>();
		Movie m1 = new Movie("1", "DS1");
		m1.setDate(DateTime.parse("1980-10-10"));
		ds.addRecord(m1);
		Movie m2 = new Movie("2", "DS1");
		m2.setDate(DateTime.parse("1990-10-10"));
		ds.addRecord(m2);
		Movie m3 = new Movie("3", "DS1");
		m3.setDate(DateTime.parse("1991-10-10"));
		ds.addRecord(m3);
		return ds;
	}

	private DataSet<Movie, DefaultSchemaElement> generateDS2() {
		DataSet<Movie, DefaultSchemaElement> ds = new DefaultDataSet<>();
		Movie m1 = new Movie("4", "DS2");
		m1.setDate(DateTime.parse("1983-10-10"));
		ds.addRecord(m1);
		Movie m2 = new Movie("5", "DS2");
		m2.setDate(DateTime.parse("1984-10-10"));
		ds.addRecord(m2);
		Movie m3 = new Movie("6", "DS2");
		m3.setDate(DateTime.parse("1995-10-10"));
		ds.addRecord(m3);
		return ds;
	}

	public void testGeneratePairs() throws XPathExpressionException,
			ParserConfigurationException, SAXException, IOException {
		DataSet<Movie, DefaultSchemaElement> ds = generateDS1();

		DataSet<Movie, DefaultSchemaElement> ds2 = generateDS2();

		Blocker<Movie, DefaultSchemaElement> blocker = new SortedNeighbourhoodBlocker<>(
				new MovieBlockingKeyByYearGenerator(), 3);

		MatchingGoldStandard gs = new MatchingGoldStandard();
		gs.loadFromCSVFile(new File(
				"usecase/movie/goldstandard/gs_academy_awards_2_actors.csv"));

		MatchingEngine<Movie, DefaultSchemaElement> engine = new MatchingEngine<>();
		
		ResultSet<BlockedMatchable<Movie, DefaultSchemaElement>> pairs = blocker.runBlocking(ds, ds2, null, engine.getProcessingEngine());

		System.out.println("Pairs: " + pairs.size());
		System.out.println("Reduction Rate: " + blocker.getReductionRatio());

		for (BlockedMatchable<Movie, DefaultSchemaElement> p : pairs.get()) {
			System.out.println(p.getFirstRecord().getIdentifier() + " | "
					+ p.getSecondRecord().getIdentifier());
		}
		assertEquals(4, pairs.size());
	}

}
