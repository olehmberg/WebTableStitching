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
package de.uni_mannheim.informatik.dws.tnt.match.rules.refiner;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import de.uni_mannheim.informatik.dws.t2k.utils.query.Func;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.wdi.matrices.SimilarityMatrix;
import de.uni_mannheim.informatik.wdi.matrices.SparseSimilarityMatrixFactory;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;
import de.uni_mannheim.informatik.wdi.processing.DatasetIterator;
import de.uni_mannheim.informatik.wdi.processing.Group;
import de.uni_mannheim.informatik.wdi.processing.RecordKeyValueMapper;
import de.uni_mannheim.informatik.wdi.processing.RecordMapper;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class PairwiseOneToOneMapping {

	public ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> run(
			ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> correspondences, 
			DataProcessingEngine proc) {
		
		// group correspondences by table combination (pairs)
		RecordKeyValueMapper<List<Integer>, Correspondence<MatchableTableColumn, MatchableTableRow>, Correspondence<MatchableTableColumn, MatchableTableRow>> groupByTable = new RecordKeyValueMapper<List<Integer>, Correspondence<MatchableTableColumn,MatchableTableRow>, Correspondence<MatchableTableColumn,MatchableTableRow>>() {
			
			@Override
			public void mapRecord(Correspondence<MatchableTableColumn, MatchableTableRow> record,
					DatasetIterator<Pair<List<Integer>, Correspondence<MatchableTableColumn, MatchableTableRow>>> resultCollector) {

				if(record.getFirstRecord().getTableId()>record.getSecondRecord().getTableId()) {
					System.out.println("Wrong direction!");
				}
				
				resultCollector.next(new Pair<List<Integer>, Correspondence<MatchableTableColumn,MatchableTableRow>>(Q.toList(record.getFirstRecord().getTableId(), record.getSecondRecord().getTableId()), record));
				
			}
		};
		ResultSet<Group<List<Integer>, Correspondence<MatchableTableColumn, MatchableTableRow>>> pairs = proc.groupRecords(correspondences, groupByTable);
		
		// create one-to-one mapping
		RecordMapper<Group<List<Integer>, Correspondence<MatchableTableColumn, MatchableTableRow>>, Correspondence<MatchableTableColumn, MatchableTableRow>> oneToOne = new RecordMapper<Group<List<Integer>,Correspondence<MatchableTableColumn,MatchableTableRow>>, Correspondence<MatchableTableColumn,MatchableTableRow>>() {
			
			@Override
			public void mapRecord(Group<List<Integer>, Correspondence<MatchableTableColumn, MatchableTableRow>> record,
					DatasetIterator<Correspondence<MatchableTableColumn, MatchableTableRow>> resultCollector) {

				SimilarityMatrix<MatchableTableColumn> m = SimilarityMatrix.fromCorrespondences(record.getRecords().get(), new SparseSimilarityMatrixFactory());
				
				Set<MatchableTableColumn> alreadyMatched = new HashSet<>();
				
				List<MatchableTableColumn> cols1 = Q.sort(m.getFirstDimension(), new MatchableTableColumn.ColumnIndexComparator());
				
				for(final MatchableTableColumn m1 : cols1) {
					Collection<MatchableTableColumn> matches = m.getMatches(m1);
					
					if(matches.size()>0) {
						
						Collection<MatchableTableColumn> sameHeader = Q.where(matches, new Func<Boolean, MatchableTableColumn>() {

							@Override
							public Boolean invoke(MatchableTableColumn in) {
								return m1.getHeader()!=null && !"null".equals(m1.getHeader()) && m1.getHeader().equals(in.getHeader());
							}
						});
						Collection<MatchableTableColumn> available = Q.without(sameHeader, alreadyMatched);
						
						// we only remove correspondences between columns with the same header (which would confuse the conflict detection later)
						// if we find two correspondences for a column with the same header, this means the other table has two columns with the same header
						// the conflict detection will detect that two columns from one table are indirectly mapped and will try to resolve it, likely removing all correspondences 
						if(sameHeader.size()>1) {
							// so we decide for one of the mappings before the conflict detection
							// heuristically (we don't have similarity values), we keep the left-most column
							// if we had the same table twice, this would result in the correct mapping, as all columns are mapped to each other from left to right
							MatchableTableColumn keep = Q.min(available, new MatchableTableColumn.ColumnIndexProjection());
							alreadyMatched.add(keep);
							
							Collection<MatchableTableColumn> remove = Q.without(sameHeader, Q.toSet(keep));
							
							matches.removeAll(remove);
						} 

						for(MatchableTableColumn m2 : matches) {
							for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : record.getRecords().get()) {
								if(cor.getFirstRecord().equals(m1) && cor.getSecondRecord().equals(m2)) {
									resultCollector.next(cor);
								}
							}
						}
					}
				}
				
			}
		};
		return proc.transform(pairs, oneToOne);
	}

}
