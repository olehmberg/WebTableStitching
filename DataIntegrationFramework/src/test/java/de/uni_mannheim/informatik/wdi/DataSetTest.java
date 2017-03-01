package de.uni_mannheim.informatik.wdi;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.xpath.XPathExpressionException;

import org.joda.time.DateTime;
import org.xml.sax.SAXException;

import de.uni_mannheim.informatik.wdi.model.DefaultDataSet;
import de.uni_mannheim.informatik.wdi.model.DefaultRecord;
import de.uni_mannheim.informatik.wdi.model.DefaultRecordFactory;
import de.uni_mannheim.informatik.wdi.model.DefaultSchemaElement;
import de.uni_mannheim.informatik.wdi.usecase.movies.model.Actor;
import de.uni_mannheim.informatik.wdi.usecase.movies.model.Movie;
import de.uni_mannheim.informatik.wdi.usecase.movies.model.MovieFactory;
import junit.framework.TestCase;

public class DataSetTest extends TestCase {

	public void testLoadFromXML() throws XPathExpressionException, ParserConfigurationException, SAXException, IOException {
		DefaultDataSet<Movie, DefaultSchemaElement> ds = new DefaultDataSet<>();
		
		File sourceFile = new File("usecase/movie/input/actors.xml");
		
		ds.loadFromXML(sourceFile, new MovieFactory(), "/movies/movie");
		
		HashMap<String, Movie> movies = new HashMap<>();
		for(Movie movie : ds.getRecords()) {
			System.out.println(String.format("[%s] %s", movie.getIdentifier(), movie.getTitle()));
			movies.put(movie.getIdentifier(), movie);
		}
		
		assertEquals(151, ds.getRecords().size());
		
/* example entry
	<movie>
		<id>actors_1</id>
		<title>7th Heaven</title>
		<actors>
			<actor>
				<name>Janet Gaynor</name>
				<birthday>1906-01-01</birthday>
				<birthplace>Pennsylvania</birthplace>
			</actor>
		</actors>
		<date>1929-01-01</date>
	</movie>
 */
	
		Movie testMovie = movies.get("actors_1");
		assertEquals("7th Heaven", testMovie.getTitle());
		assertEquals(DateTime.parse("1929-01-01"), testMovie.getDate());
		Actor testActor = testMovie.getActors().get(0);
		assertEquals("Janet Gaynor", testActor.getName());
		assertEquals(DateTime.parse("1906-01-01"), testActor.getBirthday());
		assertEquals("Pennsylvania", testActor.getBirthplace());
		
		DefaultDataSet<DefaultRecord, DefaultSchemaElement> ds2 = new DefaultDataSet<>();
		
		Map<String, DefaultSchemaElement> nodeMapping = new HashMap<>();
		nodeMapping.put("title", Movie.TITLE);
		nodeMapping.put("date", Movie.DATE);
		ds2.loadFromXML(sourceFile, new DefaultRecordFactory("id", nodeMapping), "/movies/movie");
		
		assertEquals(151, ds2.getRecords().size());
		
		for(DefaultRecord m : ds2.getRecords()) {
			String id = m.getIdentifier();
			
			Movie movie = movies.get(id);
			
			assertEquals(movie.getTitle(), m.getValue(Movie.TITLE));
			DateTime dt = DateTime.parse(m.getValue(Movie.DATE));
			assertEquals(movie.getDate(), dt);
			
		}
	}

}
