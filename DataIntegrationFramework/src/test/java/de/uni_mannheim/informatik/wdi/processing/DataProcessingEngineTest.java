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
package de.uni_mannheim.informatik.wdi.processing;

import java.util.Arrays;
import java.util.HashSet;

import de.uni_mannheim.informatik.wdi.model.DataSet;
import de.uni_mannheim.informatik.wdi.model.DefaultDataSet;
import de.uni_mannheim.informatik.wdi.model.DefaultSchemaElement;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.usecase.movies.model.Movie;
import junit.framework.TestCase;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class DataProcessingEngineTest extends TestCase {

	/**
	 * Test method for {@link de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine#iterateDataset(de.uni_mannheim.informatik.wdi.model.BasicCollection, de.uni_mannheim.informatik.wdi.processing.DatasetIterator)}.
	 */
	public void testIterateDataset() {
		
	}

	/**
	 * Test method for {@link de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine#groupRecords(de.uni_mannheim.informatik.wdi.model.BasicCollection, de.uni_mannheim.informatik.wdi.processing.RecordKeyValueMapper)}.
	 */
	public void testGroupRecords() {
		DataProcessingEngine proc = new DataProcessingEngine();
		
		DataSet<Movie, DefaultSchemaElement> data = new DefaultDataSet<>();
		Movie m1 = new Movie("m1", "");
		m1.setTitle("A");
		Movie m2 = new Movie("m2", "");
		m2.setTitle("B");
		Movie m3 = new Movie("m3", "");
		m3.setTitle("A");
		data.add(m1);
		data.add(m2);
		data.add(m3);
		
		ResultSet<Group<String, Movie>> grouped = proc.groupRecords(data, new RecordKeyValueMapper<String, Movie, Movie>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Movie record,
					DatasetIterator<Pair<String, Movie>> resultCollector) {

				// group by title
				
				resultCollector.next(new Pair<String, Movie>(record.getTitle(), record));
				
			}
		});
		
		for(Group<String, Movie> grp : grouped.get()) {
			if(grp.getRecords().size()==1) {
				assertEquals("m2", grp.getRecords().get().iterator().next().getIdentifier());
			} else if(grp.getRecords().size()==2) {
				HashSet<String> ids = new HashSet<>();
				
				for(Movie m : grp.getRecords().get()) {
					ids.add(m.getIdentifier());
				}
				
				assertEquals(new HashSet<>(Arrays.asList(new String[] { "m1", "m3" })), ids);
			}
		}
	}

	/**
	 * Test method for {@link de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine#aggregateRecords(de.uni_mannheim.informatik.wdi.model.BasicCollection, de.uni_mannheim.informatik.wdi.processing.RecordKeyValueMapper, de.uni_mannheim.informatik.wdi.processing.DataAggregator)}.
	 */
	public void testAggregateRecords() {
		
	}

	/**
	 * Test method for {@link de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine#sort(de.uni_mannheim.informatik.wdi.model.BasicCollection, java.util.Comparator)}.
	 */
	public void testSort() {
		
	}

	/**
	 * Test method for {@link de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine#filter(de.uni_mannheim.informatik.wdi.model.BasicCollection, de.uni_mannheim.informatik.wdi.processing.Function)}.
	 */
	public void testFilter() {
		
	}

}
