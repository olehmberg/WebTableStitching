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
package de.uni_mannheim.informatik.dws.tnt.match.rules.blocking;

import java.util.Collection;

import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.DataSet;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;
import de.uni_mannheim.informatik.wdi.processing.DatasetIterator;
import de.uni_mannheim.informatik.wdi.processing.Function;
import de.uni_mannheim.informatik.wdi.processing.RecordMapper;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class ColumnIndexSchemaBlocker {

	public ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> runBlocking(
			DataSet<MatchableTableColumn, MatchableTableColumn> dataset,
			boolean isSymmetric,
			ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences,
			DataProcessingEngine engine) {
		
		Function<Integer, MatchableTableColumn> columnIndexJoin = new Function<Integer, MatchableTableColumn>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer execute(MatchableTableColumn input) {
				return input.getColumnIndex();
			}
		};
		ResultSet<Pair<MatchableTableColumn, MatchableTableColumn>> joinedColumns = engine.symmetricSelfJoin(dataset, columnIndexJoin);
		
		Function<Collection<Integer>, Pair<MatchableTableColumn, MatchableTableColumn>> columnsToJoinKey = new Function<Collection<Integer>, Pair<MatchableTableColumn,MatchableTableColumn>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Collection<Integer> execute(Pair<MatchableTableColumn, MatchableTableColumn> input) {
				return Q.toSet(input.getFirst().getColumnIndex(), input.getSecond().getColumnIndex());
			}
		};
		Function<Collection<Integer>, Correspondence<MatchableTableColumn, MatchableTableRow>> correspondenceToJoinKey = new Function<Collection<Integer>, Correspondence<MatchableTableColumn,MatchableTableRow>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Collection<Integer> execute(Correspondence<MatchableTableColumn, MatchableTableRow> input) {
				return Q.toSet(input.getFirstRecord().getColumnIndex(), input.getSecondRecord().getColumnIndex());
			}
		};
		ResultSet<Pair<Pair<MatchableTableColumn, MatchableTableColumn>, Correspondence<MatchableTableColumn, MatchableTableRow>>> columnsWithCorrespondences = engine.joinMixedTypes(joinedColumns, schemaCorrespondences, columnsToJoinKey , correspondenceToJoinKey);
		
		RecordMapper<Pair<Pair<MatchableTableColumn, MatchableTableColumn>, Correspondence<MatchableTableColumn, MatchableTableRow>>, Correspondence<MatchableTableColumn, MatchableTableRow>> joinsToResult = new RecordMapper<Pair<Pair<MatchableTableColumn,MatchableTableColumn>,Correspondence<MatchableTableColumn,MatchableTableRow>>, Correspondence<MatchableTableColumn,MatchableTableRow>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(
					Pair<Pair<MatchableTableColumn, MatchableTableColumn>, Correspondence<MatchableTableColumn, MatchableTableRow>> record,
					DatasetIterator<Correspondence<MatchableTableColumn, MatchableTableRow>> resultCollector) {
				
				MatchableTableColumn c1 = record.getFirst().getFirst();
				MatchableTableColumn c2 = record.getFirst().getSecond();
				
				if(c1.getTableId()>c2.getTableId()) {
					MatchableTableColumn tmp = c1;
					c1 = c2;
					c2 = tmp;
				}
				
				resultCollector.next(new Correspondence<MatchableTableColumn, MatchableTableRow>(c1, c2, 1.0, null));
			}
		};
		return engine.transform(columnsWithCorrespondences, joinsToResult);
	}
	
}
