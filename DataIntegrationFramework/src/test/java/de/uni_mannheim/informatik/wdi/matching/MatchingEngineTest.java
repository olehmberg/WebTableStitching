package de.uni_mannheim.informatik.wdi.matching;

import java.io.File;

import junit.framework.TestCase;
import de.uni_mannheim.informatik.wdi.matching.blocking.BlockedMatchable;
import de.uni_mannheim.informatik.wdi.matching.blocking.Blocker;
import de.uni_mannheim.informatik.wdi.matching.blocking.SchemaBlocker;
import de.uni_mannheim.informatik.wdi.matching.blocking.StandardBlocker;
import de.uni_mannheim.informatik.wdi.matching.blocking.StaticBlockingKeyGenerator;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.DataSet;
import de.uni_mannheim.informatik.wdi.model.DefaultDataSet;
import de.uni_mannheim.informatik.wdi.model.DefaultSchemaElement;
import de.uni_mannheim.informatik.wdi.model.FeatureVectorDataSet;
import de.uni_mannheim.informatik.wdi.model.MatchingGoldStandard;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;
import de.uni_mannheim.informatik.wdi.usecase.movies.identityresolution.MovieDateComparator10Years;
import de.uni_mannheim.informatik.wdi.usecase.movies.identityresolution.MovieDirectorComparatorLevenshtein;
import de.uni_mannheim.informatik.wdi.usecase.movies.identityresolution.MovieTitleComparatorLevenshtein;
import de.uni_mannheim.informatik.wdi.usecase.movies.model.Movie;
import de.uni_mannheim.informatik.wdi.usecase.movies.model.MovieFactory;

public class MatchingEngineTest extends TestCase {

	public void testRunMatching() throws Exception {
		DefaultDataSet<Movie, DefaultSchemaElement> ds = new DefaultDataSet<>();
		File sourceFile1 = new File("usecase/movie/input/actors.xml");
		ds.loadFromXML(sourceFile1, new MovieFactory(), "/movies/movie");

		DefaultDataSet<Movie, DefaultSchemaElement> ds2 = new DefaultDataSet<>();
		File sourceFile2 = new File("usecase/movie/input/academy_awards.xml");
		ds2.loadFromXML(sourceFile2, new MovieFactory(), "/movies/movie");

		LinearCombinationMatchingRule<Movie, DefaultSchemaElement> rule = new LinearCombinationMatchingRule<>(
				0, 0);
		rule.addComparator(new MovieTitleComparatorLevenshtein(), 0.5);
		rule.addComparator(new MovieDirectorComparatorLevenshtein(), 0.25);
		rule.addComparator(new MovieDateComparator10Years(), 0.25);

		Blocker<Movie, DefaultSchemaElement> blocker = new StandardBlocker<>(
				new StaticBlockingKeyGenerator<Movie>());
		MatchingEngine<Movie, DefaultSchemaElement> engine = new MatchingEngine<>();

		engine.runIdentityResolution(ds, ds2, null, rule, blocker);
	}

	public void testRunDeduplication() throws Exception {
		DefaultDataSet<Movie, DefaultSchemaElement> ds = new DefaultDataSet<>();
		File sourceFile1 = new File("usecase/movie/input/actors.xml");
		ds.loadFromXML(sourceFile1, new MovieFactory(), "/movies/movie");

		LinearCombinationMatchingRule<Movie, DefaultSchemaElement> rule = new LinearCombinationMatchingRule<>(
				0, 0);
		rule.addComparator(new MovieTitleComparatorLevenshtein(), 0.5);
		rule.addComparator(new MovieDirectorComparatorLevenshtein(), 0.25);
		rule.addComparator(new MovieDateComparator10Years(), 0.25);

		Blocker<Movie, DefaultSchemaElement> blocker = new StandardBlocker<>(
				new StaticBlockingKeyGenerator<Movie>());
		MatchingEngine<Movie, DefaultSchemaElement> engine = new MatchingEngine<>();

		engine.runDuplicateDetection(ds, true, null, rule, blocker);
	}

	public void testGenerateFeaturesForOptimisation()
			throws Exception {
		DefaultDataSet<Movie, DefaultSchemaElement> ds = new DefaultDataSet<>();
		File sourceFile1 = new File("usecase/movie/input/actors.xml");
		ds.loadFromXML(sourceFile1, new MovieFactory(), "/movies/movie");

		DefaultDataSet<Movie, DefaultSchemaElement> ds2 = new DefaultDataSet<>();
		File sourceFile2 = new File("usecase/movie/input/academy_awards.xml");
		ds2.loadFromXML(sourceFile2, new MovieFactory(), "/movies/movie");

		LinearCombinationMatchingRule<Movie, DefaultSchemaElement> rule = new LinearCombinationMatchingRule<>(
				0, 0);
		rule.addComparator(new MovieTitleComparatorLevenshtein(), 0.5);
		rule.addComparator(new MovieDirectorComparatorLevenshtein(), 0.25);
		rule.addComparator(new MovieDateComparator10Years(), 0.25);

		MatchingEngine<Movie, DefaultSchemaElement> engine = new MatchingEngine<>();

		MatchingGoldStandard gs = new MatchingGoldStandard();
		gs.loadFromCSVFile(new File(
				"usecase/movie/goldstandard/gs_academy_awards_2_actors.csv"));

		FeatureVectorDataSet features = new FeatureVectorDataSet();
		engine.generateTrainingDataForLearning(ds, ds2, gs, rule, null, features);
	}

	public void testRunSchemaMatching() throws Exception {
		DefaultDataSet<Movie, DefaultSchemaElement> ds1 = new DefaultDataSet<>();
		ds1.addAttribute(new DefaultSchemaElement("att1"));
		ds1.addAttribute(new DefaultSchemaElement("att2"));
		DefaultDataSet<Movie, DefaultSchemaElement> ds2 = new DefaultDataSet<>();
		ds2.addAttribute(new DefaultSchemaElement("att1"));
		ds2.addAttribute(new DefaultSchemaElement("att2"));
		
		LinearCombinationMatchingRule<DefaultSchemaElement, Movie> rule = new LinearCombinationMatchingRule<>(1.0);
		Comparator<DefaultSchemaElement, Movie> comp = new Comparator<DefaultSchemaElement, Movie>() {
			private static final long serialVersionUID = 1L;

			@Override
			public double compare(
					DefaultSchemaElement record1,
					DefaultSchemaElement record2,
					Correspondence<Movie, DefaultSchemaElement> schemaCorrespondences) {
				return record1.getIdentifier().equals(record2.getIdentifier()) ? 1.0 : 0.0;
			}
		};
		rule.addComparator(comp, 1.0);
		
		SchemaBlocker<DefaultSchemaElement, Movie> blocker = new SchemaBlocker<DefaultSchemaElement, Movie>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void initialise(
					DataSet<DefaultSchemaElement, DefaultSchemaElement> dataset,
					boolean isSymmetric,
					ResultSet<Correspondence<Movie, DefaultSchemaElement>> instanceCorrespondences) {
				
			}
			
			@Override
			public void initialise(
					DataSet<DefaultSchemaElement, DefaultSchemaElement> schema1,
					DataSet<DefaultSchemaElement, DefaultSchemaElement> schema2,
					ResultSet<Correspondence<Movie, DefaultSchemaElement>> instanceCorrespondences) {
				
			}
			
			/* (non-Javadoc)
			 * @see de.uni_mannheim.informatik.wdi.matching.blocking.SchemaBlocker#runBlocking(de.uni_mannheim.informatik.wdi.model.DataSet, de.uni_mannheim.informatik.wdi.model.DataSet, de.uni_mannheim.informatik.wdi.model.ResultSet, de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine)
			 */
			@Override
			public ResultSet<BlockedMatchable<DefaultSchemaElement, Movie>> runBlocking(
					DataSet<DefaultSchemaElement, DefaultSchemaElement> schema1,
					DataSet<DefaultSchemaElement, DefaultSchemaElement> schema2,
					ResultSet<Correspondence<Movie, DefaultSchemaElement>> instanceCorrespondences,
					DataProcessingEngine engine) {
				ResultSet<BlockedMatchable<DefaultSchemaElement, Movie>> result = new ResultSet<>();
				
				for(DefaultSchemaElement s1 : schema1.get()) {
					for(DefaultSchemaElement s2 : schema2.get()) {
						result.add(new MatchingTask<DefaultSchemaElement, Movie>(s1, s2, instanceCorrespondences));
					}
				}
				
				return result;
			}
		};
		
		MatchingEngine<Movie, DefaultSchemaElement> engine = new MatchingEngine<>();
		DefaultDataSet<DefaultSchemaElement, DefaultSchemaElement> schema1 = new DefaultDataSet<>();
		for(DefaultSchemaElement s : ds1.getAttributes()) {
			schema1.add(s);
		}
		DefaultDataSet<DefaultSchemaElement, DefaultSchemaElement> schema2 = new DefaultDataSet<>();
		for(DefaultSchemaElement s : ds2.getAttributes()) {
			schema2.add(s);
		}
		ResultSet<Correspondence<DefaultSchemaElement, Movie>> result = engine.runSchemaMatching(schema1, schema2, null, rule, blocker);
		
		for(Correspondence<DefaultSchemaElement, Movie> cor : result.get()) {
			assertEquals(cor.getFirstRecord().getIdentifier(), cor.getSecondRecord().getIdentifier());
		}
	}
	
}
