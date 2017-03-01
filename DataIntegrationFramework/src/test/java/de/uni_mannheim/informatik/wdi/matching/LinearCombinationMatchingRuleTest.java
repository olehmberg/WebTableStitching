package de.uni_mannheim.informatik.wdi.matching;

import junit.framework.TestCase;

import org.joda.time.DateTime;

import de.uni_mannheim.informatik.wdi.model.DefaultSchemaElement;
import de.uni_mannheim.informatik.wdi.usecase.movies.identityresolution.MovieDateComparator10Years;
import de.uni_mannheim.informatik.wdi.usecase.movies.identityresolution.MovieDirectorComparatorLevenshtein;
import de.uni_mannheim.informatik.wdi.usecase.movies.identityresolution.MovieTitleComparatorLevenshtein;
import de.uni_mannheim.informatik.wdi.usecase.movies.model.Movie;

public class LinearCombinationMatchingRuleTest extends TestCase {

	public void testApply() throws Exception {
		Movie movie1 = new Movie("movie1", "test");
		Movie movie2 = new Movie("movie2", "test");
		Movie movie3 = new Movie("movie3", "test");
		
		movie1.setTitle("Star Wars IV");
		movie2.setTitle("Star Wars V");
		movie3.setTitle("Star Wars IV");
		
		movie1.setDirector("George Lucas");
		movie2.setDirector("Irvin Kershner");
		movie3.setDirector("Irvin Kershner");
		
		movie1.setDate(DateTime.parse("1977-05-25"));
		movie2.setDate(DateTime.parse("1980-05-21"));
		movie3.setDate(DateTime.parse("1977-05-25"));
		
		LinearCombinationMatchingRule<Movie, DefaultSchemaElement> rule1 = new LinearCombinationMatchingRule<>(0.0, 1.0);
		rule1.addComparator(new MovieTitleComparatorLevenshtein(), 1.0);
		assertNotNull(rule1.apply(movie1, movie3, null));
		assertNull(rule1.apply(movie1, movie2, null));
		
		LinearCombinationMatchingRule<Movie, DefaultSchemaElement> rule2 = new LinearCombinationMatchingRule<>(0.0, 0.9);
		rule2.addComparator(new MovieTitleComparatorLevenshtein(), 0.1);
		rule2.addComparator(new MovieDirectorComparatorLevenshtein(), 0.9);
		assertNotNull(rule2.apply(movie2, movie3, null));
		assertNull(rule2.apply(movie1, movie2, null));
		
		LinearCombinationMatchingRule<Movie, DefaultSchemaElement> rule3 = new LinearCombinationMatchingRule<>(0.0, 0.8);
		rule3.addComparator(new MovieTitleComparatorLevenshtein(), 0.1);
		rule3.addComparator(new MovieDirectorComparatorLevenshtein(), 0.1);
		rule3.addComparator(new MovieDateComparator10Years(), 0.8);
		assertNotNull(rule3.apply(movie1, movie3, null));
		assertNull(rule3.apply(movie2, movie3, null));		
	}

}
