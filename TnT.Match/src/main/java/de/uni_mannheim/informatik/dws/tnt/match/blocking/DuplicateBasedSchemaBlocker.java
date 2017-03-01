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
package de.uni_mannheim.informatik.dws.tnt.match.blocking;

import java.util.Collection;

import de.uni_mannheim.informatik.dws.t2k.utils.query.Func;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.tnt.match.SpecialColumns;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.wdi.matching.MatchingTask;
import de.uni_mannheim.informatik.wdi.matching.blocking.BlockedMatchable;
import de.uni_mannheim.informatik.wdi.matching.blocking.SchemaBlocker;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.DataSet;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.model.Triple;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;
import de.uni_mannheim.informatik.wdi.processing.DatasetIterator;
import de.uni_mannheim.informatik.wdi.processing.Function;
import de.uni_mannheim.informatik.wdi.processing.Group;
import de.uni_mannheim.informatik.wdi.processing.RecordKeyValueMapper;
import de.uni_mannheim.informatik.wdi.processing.RecordMapper;

/**
 * 
 * Blocks MatchableTableColumns based on instance correspondences
 * Only columns from tables which have an instance correspondence will be compared
 * Only the instance correspondences relevant for the columns will be added to the matching task
 * Special Columns will be ignored
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class DuplicateBasedSchemaBlocker extends SchemaBlocker<MatchableTableColumn, MatchableTableRow> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.matching.blocking.SchemaBlocker#initialise(de.uni_mannheim.informatik.wdi.model.DataSet, de.uni_mannheim.informatik.wdi.model.DataSet, de.uni_mannheim.informatik.wdi.model.ResultSet)
	 */
	@Override
	public void initialise(DataSet<MatchableTableColumn, MatchableTableColumn> schema1,
			DataSet<MatchableTableColumn, MatchableTableColumn> schema2,
			ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.matching.blocking.SchemaBlocker#initialise(de.uni_mannheim.informatik.wdi.model.DataSet, boolean, de.uni_mannheim.informatik.wdi.model.ResultSet)
	 */
	@Override
	public void initialise(DataSet<MatchableTableColumn, MatchableTableColumn> dataset, boolean isSymmetric,
			ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences) {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.matching.blocking.SchemaBlocker#runBlocking(de.uni_mannheim.informatik.wdi.model.DataSet, de.uni_mannheim.informatik.wdi.model.DataSet, de.uni_mannheim.informatik.wdi.model.ResultSet, de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine)
	 */
	@Override
	public ResultSet<BlockedMatchable<MatchableTableColumn, MatchableTableRow>> runBlocking(
			DataSet<MatchableTableColumn, MatchableTableColumn> schema1,
			DataSet<MatchableTableColumn, MatchableTableColumn> schema2,
			ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences,
			DataProcessingEngine engine) {

		// filter out special columns
		ResultSet<MatchableTableColumn> schema1Cols = engine.filter(schema1, new Function<Boolean, MatchableTableColumn>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean execute(MatchableTableColumn input) {
				return input.getColumnIndex() >= SpecialColumns.ALL.size();
			}});

		ResultSet<MatchableTableColumn> schema2Cols = engine.filter(schema2, new Function<Boolean, MatchableTableColumn>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Boolean execute(MatchableTableColumn input) {
				return input.getColumnIndex() >= SpecialColumns.ALL.size();
			}});
		

		// join schemas and instance correspondences
		// join schema1 with left side of instance correspondences
		ResultSet<Pair<MatchableTableColumn, Correspondence<MatchableTableRow, MatchableTableColumn>>> firstJoin = engine.joinMixedTypes(schema1Cols, instanceCorrespondences, 
				new Function<Integer, MatchableTableColumn>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer execute(MatchableTableColumn input) {
				return input.getTableId();
			}}, 
				new Function<Integer, Correspondence<MatchableTableRow, MatchableTableColumn>>() {

				/**
				 * 
				 */
				private static final long serialVersionUID = 1L;

				@Override
				public Integer execute(Correspondence<MatchableTableRow, MatchableTableColumn> input) {
					return input.getFirstRecord().getTableId();
				}
		});
		
		// join result with schema2 via right side of instance correspondences
		 ResultSet<Pair<Pair<MatchableTableColumn, Correspondence<MatchableTableRow, MatchableTableColumn>>, MatchableTableColumn>> secondJoin = engine.joinMixedTypes(firstJoin, schema2Cols, 
				new Function<Integer, Pair<MatchableTableColumn, Correspondence<MatchableTableRow, MatchableTableColumn>>>() {

			/**
					 * 
					 */
					private static final long serialVersionUID = 1L;

			@Override
			public Integer execute(
					Pair<MatchableTableColumn, Correspondence<MatchableTableRow, MatchableTableColumn>> input) {
				return input.getSecond().getSecondRecord().getTableId();
			}}, new Function<Integer, MatchableTableColumn>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer execute(MatchableTableColumn input) {
				return input.getTableId();
			}});
		
		 // group by both schema elements (so we get a list of all instance correspondences which are relevant for them)
		 ResultSet<Group<String, Triple<MatchableTableColumn, MatchableTableColumn, Correspondence<MatchableTableRow, MatchableTableColumn>>>> grouped = engine.groupRecords(secondJoin, 
				 new RecordKeyValueMapper<
				 	String, 
				 	Pair<Pair<MatchableTableColumn, Correspondence<MatchableTableRow, MatchableTableColumn>>, MatchableTableColumn>, 
				 	Triple<MatchableTableColumn, MatchableTableColumn, Correspondence<MatchableTableRow, MatchableTableColumn>>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(
					Pair<Pair<MatchableTableColumn, Correspondence<MatchableTableRow, MatchableTableColumn>>, MatchableTableColumn> record,
					DatasetIterator<Pair<String, Triple<MatchableTableColumn, MatchableTableColumn, Correspondence<MatchableTableRow, MatchableTableColumn>>>> resultCollector) {

				Correspondence<MatchableTableRow, MatchableTableColumn> cor = record.getFirst().getSecond();
				MatchableTableColumn col1 = record.getFirst().getFirst();
				MatchableTableColumn col2 = record.getSecond();
				
				Triple<MatchableTableColumn, MatchableTableColumn, Correspondence<MatchableTableRow, MatchableTableColumn>> value = new Triple<>(col1, col2, cor);
				
				String key = String.format("%d.%d/%d.%d", col1.getTableId(), col1.getColumnIndex(), col2.getTableId(), col2.getColumnIndex());
				
				if(col1.getTableId()!=col2.getTableId()) {
					resultCollector.next(new Pair<>(key, value));
				}
			}


		});
		 
		 // transform the grouping result into BlockedMatchable instances
		 ResultSet<BlockedMatchable<MatchableTableColumn, MatchableTableRow>> results = engine.transform(grouped, new RecordMapper<Group<String, Triple<MatchableTableColumn, MatchableTableColumn, Correspondence<MatchableTableRow, MatchableTableColumn>>>, BlockedMatchable<MatchableTableColumn, MatchableTableRow>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Group<String, Triple<MatchableTableColumn, MatchableTableColumn, Correspondence<MatchableTableRow, MatchableTableColumn>>> record,
					DatasetIterator<BlockedMatchable<MatchableTableColumn, MatchableTableRow>> resultCollector) {
				
				Collection<Triple<MatchableTableColumn, MatchableTableColumn, Correspondence<MatchableTableRow, MatchableTableColumn>>> all = record.getRecords().get();
				
				Triple<MatchableTableColumn, MatchableTableColumn, Correspondence<MatchableTableRow, MatchableTableColumn>> first = all.iterator().next();
				
				MatchableTableColumn col1 = first.getFirst();
				MatchableTableColumn col2 = first.getSecond();
				
				Collection<Correspondence<MatchableTableRow, MatchableTableColumn>> cors = Q.project(all, new Func<Correspondence<MatchableTableRow, MatchableTableColumn>, Triple<MatchableTableColumn, MatchableTableColumn, Correspondence<MatchableTableRow, MatchableTableColumn>>>(){

					@Override
					public Correspondence<MatchableTableRow, MatchableTableColumn> invoke(
							Triple<MatchableTableColumn, MatchableTableColumn, Correspondence<MatchableTableRow, MatchableTableColumn>> in) {
						return in.getThird();
					}});
				
				
				BlockedMatchable<MatchableTableColumn, MatchableTableRow> bm = new MatchingTask<MatchableTableColumn, MatchableTableRow>(col1, col2, new ResultSet<>(cors));
				
				resultCollector.next(bm);
			}
		});
		 
		return results;
	}

}
