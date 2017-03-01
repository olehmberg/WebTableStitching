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
package de.uni_mannheim.informatik.wdi.spark.processing;

import java.io.Serializable;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import de.uni_mannheim.informatik.wdi.model.DataSet;
import de.uni_mannheim.informatik.wdi.model.DefaultSchemaElement;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DataAggregator;
import de.uni_mannheim.informatik.wdi.processing.DatasetIterator;
import de.uni_mannheim.informatik.wdi.processing.RecordKeyValueMapper;
import de.uni_mannheim.informatik.wdi.spark.SparkDataSet;
import de.uni_mannheim.informatik.wdi.usecase.movies.model.Movie;
import junit.framework.TestCase;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class SparkDataProcessingEngineTest extends TestCase implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * Test method for {@link de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine#iterateDataset(de.uni_mannheim.informatik.wdi.model.BasicCollection, de.uni_mannheim.informatik.wdi.processing.DatasetIterator)}.
	 */
	public void testIterateDataset() {
//		SparkConf conf = new SparkConf().setAppName("LineCount").setMaster("local[4]");
//    	JavaSparkContext sc = new JavaSparkContext(conf);
//    	
//		DataSet<Movie, DefaultSchemaElement> ds = new SparkDataSet<>(sc);
//		Movie m1 = new Movie("m1", "");
//		m1.setDirector("test");
//		Movie m2 = new Movie("m2", "");
//		m2.setDirector("not test");
//		Movie m3 = new Movie("m3", "");
//		m3.setDirector("not test");
//		ds.add(m1);
//		ds.add(m2);
//		ds.add(m3);
//		
//		SparkDataProcessingEngine proc = new SparkDataProcessingEngine(sc);
//		proc.iterateDataset(ds, new DatasetIterator<Movie>() {
//
//			/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public void initialise() {
//				// TODO Auto-generated method stub
//				
//			}
//
//			@Override
//			public void next(Movie record) {
//				record.setTitle("done");
//				
//			}
//
//			@Override
//			public void finalise() {
//				// TODO Auto-generated method stub
//				
//			}
//
//		});
//		
//		for(Movie m : ds.getRecords()){
//			System.out.println(m.getTitle());
//		}
		
	}

	/**
	 * Test method for {@link de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine#transform(de.uni_mannheim.informatik.wdi.model.BasicCollection, de.uni_mannheim.informatik.wdi.processing.RecordMapper)}.
	 */
	public void testTransform() {
//		SparkConf conf = new SparkConf().setAppName("LineCount").setMaster("local[4]");
//    	JavaSparkContext sc = new JavaSparkContext(conf);
//    	
//		DataSet<Movie, DefaultSchemaElement> ds1 = new SparkDataSet<>(sc);
//		Movie m1 = new Movie("m1", "");
//		m1.setDirector("test");
//		Movie m2 = new Movie("m2", "");
//		m2.setDirector("not test");
//		Movie m3 = new Movie("m3", "");
//		m3.setDirector("not test");
//		ds1.add(m1);
//		ds1.add(m2);
//		ds1.add(m3);
//		
//		SparkDataProcessingEngine proc = new SparkDataProcessingEngine(sc);
//		
//		ResultSet<Pair<Movie, Movie>> result = proc.transform(ds1, new RecordMapper<Movie, Pair<Movie, Movie>>() {
//
//			/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public void mapRecord(Movie record,
//					DatasetIterator<Pair<Movie, Movie>> resultCollector) {
//				resultCollector.next(new Pair<Movie, Movie>(record, record));			
//			}
//
//		});
//		
//		for(Pair<Movie, Movie> pair : result.get()){
//			System.out.println(pair.getFirst().getDirector() + " " + pair.getSecond().getDirector());
//		}
	}

	/**
	 * Test method for {@link de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine#symmetricSelfJoin(de.uni_mannheim.informatik.wdi.model.BasicCollection, de.uni_mannheim.informatik.wdi.processing.Function)}.
	 */
	public void testSymmetricSelfJoin() {
//		SparkConf conf = new SparkConf().setAppName("LineCount").setMaster("local[4]");
//    	JavaSparkContext sc = new JavaSparkContext(conf);
//    	
//		DataSet<Movie, DefaultSchemaElement> ds1 = new SparkDataSet<>(sc);
//		Movie m1 = new Movie("m1", "");
//		m1.setDirector("test");
//		Movie m2 = new Movie("m2", "");
//		m2.setDirector("not test");
//		Movie m3 = new Movie("m3", "");
//		m3.setDirector("not test");
//		Movie m4 = new Movie("m4", "");
//		m4.setDirector("test");
//		Movie m5 = new Movie("m5", "");
//		m5.setDirector("not test");
//		Movie m6 = new Movie("m6", "");
//		m6.setDirector("non test");
//		ds1.add(m1);
//		ds1.add(m2);
//		ds1.add(m3);
//		ds1.add(m4);
//		ds1.add(m5);
//		ds1.add(m6);
//		
//		SparkDataProcessingEngine proc = new SparkDataProcessingEngine(sc);
//		
//		ResultSet<Pair<Movie, Movie>> result = proc.symmetricSelfJoin(ds1, new Function<String, Movie>() {
//
//			/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public String execute(Movie input) {
//				
//				return input.getDirector();
//			}
//		});
//		
//		for(Pair<Movie, Movie> m : result.get()){
//			System.out.println(m.getFirst().getIdentifier() + " " + m.getSecond().getIdentifier() + " " + m.getFirst().getDirector());
//		}
	}

	/**
	 * Test method for {@link de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine#join(de.uni_mannheim.informatik.wdi.model.BasicCollection, de.uni_mannheim.informatik.wdi.model.BasicCollection, de.uni_mannheim.informatik.wdi.processing.Function)}.
	 */
	public void testJoinBasicCollectionOfRecordTypeBasicCollectionOfRecordTypeFunctionOfKeyTypeRecordType() {
//		SparkConf conf = new SparkConf().setAppName("LineCount").setMaster("local[4]");
//    	JavaSparkContext sc = new JavaSparkContext(conf);
//    	
//		DataSet<Movie, DefaultSchemaElement> ds1 = new SparkDataSet<>(sc);
//		Movie m1 = new Movie("m1", "");
//		m1.setDirector("test");
//		Movie m2 = new Movie("m2", "");
//		m2.setDirector("not test");
//		Movie m3 = new Movie("m3", "");
//		m3.setDirector("not test");
//		ds1.add(m1);
//		ds1.add(m2);
//		ds1.add(m3);
//		
//		DataSet<Movie, DefaultSchemaElement> ds2 = new SparkDataSet<>(sc);
//		Movie n1 = new Movie("n1", "");
//		m1.setDirector("test");
//		Movie n2 = new Movie("n2", "");
//		m2.setDirector("not test");
//		Movie n3 = new Movie("n3", "");
//		m3.setDirector("not test");
//		ds2.add(m1);
//		ds2.add(m2);
//		ds2.add(m3);
//		
//		SparkDataProcessingEngine proc = new SparkDataProcessingEngine(sc);
//		
//		ResultSet<Pair<Movie, Movie>> result = proc.join(ds1, ds2, new Function<String, Movie>() {
//
//			/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public String execute(Movie input) {
//				
//				return input.getDirector();
//			}
//		});
//		
//		for(Pair<Movie, Movie> pair : result.get()){
//			System.out.println(pair.getFirst().getDirector() + " " + pair.getSecond().getDirector());
//		}
	}

	/**
	 * Test method for {@link de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine#join(de.uni_mannheim.informatik.wdi.model.BasicCollection, de.uni_mannheim.informatik.wdi.model.BasicCollection, de.uni_mannheim.informatik.wdi.processing.Function, de.uni_mannheim.informatik.wdi.processing.Function)}.
	 */
	public void testJoinBasicCollectionOfRecordTypeBasicCollectionOfRecordTypeFunctionOfKeyTypeRecordTypeFunctionOfKeyTypeRecordType() {
		
	}

	/**
	 * Test method for {@link de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine#groupRecords(de.uni_mannheim.informatik.wdi.model.BasicCollection, de.uni_mannheim.informatik.wdi.processing.RecordKeyValueMapper)}.
	 */
	public void testGroupRecords() {
//		SparkConf conf = new SparkConf().setAppName("LineCount").setMaster("local[4]");
//    	JavaSparkContext sc = new JavaSparkContext(conf);
//    	
//		DataSet<Movie, DefaultSchemaElement> ds1 = new SparkDataSet<>(sc);
//		Movie m1 = new Movie("m1", "");
//		m1.setDirector("test");
//		Movie m2 = new Movie("m2", "");
//		m2.setDirector("not test");
//		Movie m3 = new Movie("m3", "");
//		m3.setDirector("not test");
//		Movie m4 = new Movie("m4", "");
//		m4.setDirector("test");
//		Movie m5 = new Movie("m5", "");
//		m5.setDirector("not test");
//		Movie m6 = new Movie("m6", "");
//		m6.setDirector("non test");
//		ds1.add(m1);
//		ds1.add(m2);
//		ds1.add(m3);
//		ds1.add(m4);
//		ds1.add(m5);
//		ds1.add(m6);
//		
//		SparkDataProcessingEngine proc = new SparkDataProcessingEngine(sc);
//		
//		ResultSet<Group<String, Movie>> result = proc.groupRecords(ds1, new RecordKeyValueMapper<String, Movie, Movie>() {
//
//			/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public void mapRecord(Movie record,
//					DatasetIterator<Pair<String, Movie>> resultCollector) {
////				resultCollector.next(new Pair<String, Movie>(record.getDirector(), record));
//				resultCollector.next(new Pair<String, Movie>(record.getIdentifier(), record));
//			}
//		});
//		
//		for(Group<String, Movie> g : result.get()){
//			System.out.println(g.getKey() + " " + g.getRecords().size());
//		}
	}

	/**
	 * Test method for {@link de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine#aggregateRecords(de.uni_mannheim.informatik.wdi.model.BasicCollection, de.uni_mannheim.informatik.wdi.processing.RecordKeyValueMapper, de.uni_mannheim.informatik.wdi.processing.DataAggregator)}.
	 */
	public void testAggregateRecords() {
		SparkConf conf = new SparkConf().setAppName("LineCount").setMaster("local[4]");
    	JavaSparkContext sc = new JavaSparkContext(conf);
    	
		DataSet<Movie, DefaultSchemaElement> ds1 = new SparkDataSet<>(sc);
		Movie m1 = new Movie("m1", "");
		m1.setDirector("test");
		Movie m2 = new Movie("m2", "");
		m2.setDirector("not test");
		Movie m3 = new Movie("m3", "");
		m3.setDirector("not test");
		Movie m4 = new Movie("m4", "");
		m4.setDirector("test");
		Movie m5 = new Movie("m5", "");
		m5.setDirector("not test");
		Movie m6 = new Movie("m6", "");
		m6.setDirector("non test");
		ds1.add(m1);
		ds1.add(m2);
		ds1.add(m3);
		ds1.add(m4);
		ds1.add(m5);
		ds1.add(m6);
		
		SparkDataProcessingEngine proc = new SparkDataProcessingEngine(sc);
		
		ResultSet<Pair<String, String>> result = proc.aggregateRecords(ds1, new RecordKeyValueMapper<String, Movie, Movie>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Movie record,
					DatasetIterator<Pair<String, Movie>> resultCollector) {
				resultCollector.next(new Pair<String, Movie>(record.getDirector(), record));
				
			}
		}, new DataAggregator<String, Movie, String>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public String aggregate(String previousResult, Movie record) {
				if(previousResult != null)
					return previousResult.concat(" " + record.getDirector());
				else 
					return record.getDirector();
			}

			@Override
			public String initialise(String keyValue) {
				return null;
			}
		});
		
		for(Pair<String, String> pair : result.get()){
			System.out.println(pair.getFirst() + " " + pair.getSecond());
		}
		
	}

	/**
	 * Test method for {@link de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine#sort(de.uni_mannheim.informatik.wdi.model.BasicCollection, java.util.Comparator)}.
	 */
	public void testSort() {
//		SparkConf conf = new SparkConf().setAppName("LineCount").setMaster("local[4]");
//    	JavaSparkContext sc = new JavaSparkContext(conf);
//    	
//		DataSet<Movie, DefaultSchemaElement> ds1 = new SparkDataSet<>(sc);
//		Movie m1 = new Movie("m1", "");
//		m1.setDirector("test");
//		Movie m2 = new Movie("m2", "");
//		m2.setDirector("not test");
//		Movie m3 = new Movie("m3", "");
//		m3.setDirector("not test");
//		ds1.add(m1);
//		ds1.add(m2);
//		ds1.add(m3);
//		
//		SparkDataProcessingEngine proc = new SparkDataProcessingEngine(sc);
//		
//		ResultSet<Movie> result = proc.sort(ds1, new Function<String, Movie>() {
//
//			/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public String execute(Movie input) {		
//				return input.getIdentifier();
//			}
//		}, false);
//		
//		for(Movie m : result.get()){
//			System.out.println(m.getIdentifier());
//		}
	}

	/**
	 * Test method for {@link de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine#filter(de.uni_mannheim.informatik.wdi.model.BasicCollection, de.uni_mannheim.informatik.wdi.processing.Function)}.
	 */
	public void testFilter() {
//		SparkConf conf = new SparkConf().setAppName("LineCount").setMaster("local[4]");
//    	JavaSparkContext sc = new JavaSparkContext(conf);
//    	
//		DataSet<Movie, DefaultSchemaElement> ds = new SparkDataSet<>(sc);
//		Movie m1 = new Movie("m1", "");
//		m1.setDirector("test");
//		Movie m2 = new Movie("m2", "");
//		m2.setDirector("not test");
//		Movie m3 = new Movie("m3", "");
//		m3.setDirector("not test");
//		ds.add(m1);
//		ds.add(m2);
//		ds.add(m3);
//		
//		SparkDataProcessingEngine proc = new SparkDataProcessingEngine(sc);
//		
//		ResultSet<Movie> result = proc.filter(ds, new Function<Boolean, Movie>() {
//
//			/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public Boolean execute(Movie input) {
//				return input.getDirector().equals("test");
//			}
//		});
//		
//		for(Movie m : result.get()) {
//			if(!m.getDirector().equals("test")) {
//				fail("Incorrect data after filter");
//			}
//		}
//		for(Movie m : result.get()) {
//			if(!m.getDirector().equals("not test")) {
//				System.out.println(m.getIdentifier());
//			}
//		}
//		
	}

}
