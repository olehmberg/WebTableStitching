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
package de.uni_mannheim.informatik.dws.tnt.match.rules.aggregation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import de.uni_mannheim.informatik.dws.t2k.utils.query.Func;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.tnt.match.SpecialColumns;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableKey;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.wdi.matrices.SimilarityMatrix;
import de.uni_mannheim.informatik.wdi.matrices.SparseSimilarityMatrixFactory;
import de.uni_mannheim.informatik.wdi.matrices.matcher.BestChoiceMatching;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;
import de.uni_mannheim.informatik.wdi.processing.DatasetIterator;
import de.uni_mannheim.informatik.wdi.processing.Function;
import de.uni_mannheim.informatik.wdi.processing.Group;
import de.uni_mannheim.informatik.wdi.processing.RecordKeyValueMapper;
import de.uni_mannheim.informatik.wdi.processing.RecordMapper;

/**
 * 
 * Aggregates the given instance correspondences to schema correspondences by voting.
 * 
 * All instance correspondences for the same combination of tables AND attribute combinations (that created the instance correspondences) participate in the same voting.
 * I.e., if for a table combination T1, T2 two different attribute combinations, T1.{A,B}<->T2.{C,D} and T1.{E}<->T2.{F}, were used to create record links, there will be two votings in which schema correspondences are generated:
 *   1) one for T1.{A,B}<->T2.{C,D}
 *   2) and one for T1.{E}<->T2.{F}
 * 
 * The similarity value of the correspondences is the size of the attribute combinations: 
 * 	 1) 2 for T1.{A,B}<->T2.{C,D}
 *   2) 1 for T1.{E}<->T2.{F}
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class InstanceByKeyToSchemaAggregator {
	
	private int minVotes = 0;
	private boolean noFiltering = false;
	private boolean uncertainOnly = false;
	private double minSimilarity = 1.0;
	private boolean mustIncludeDependant = false;
	
	public InstanceByKeyToSchemaAggregator(int minVotes, boolean noFiltering, boolean uncertainOnly, boolean mustIncludeDependant, double minSimilarity) {
		this.minVotes = minVotes;
		this.noFiltering = noFiltering;
		this.uncertainOnly = uncertainOnly;
		this.mustIncludeDependant = mustIncludeDependant;
		this.minSimilarity = minSimilarity;
	}
	
	//TODO add schema correspondences that were used for creating the record links (otherwise the results can be randomly inconsistent)
	public ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> aggregate(ResultSet<Correspondence<MatchableTableRow, MatchableTableKey>> correspondences,
			DataProcessingEngine proc) {
		
		// group the correspondences by the table combination and the key combination
		RecordKeyValueMapper<Collection<Object>, Correspondence<MatchableTableRow, MatchableTableKey>, Correspondence<MatchableTableRow, MatchableTableKey>> groupByTablesMapper = new RecordKeyValueMapper<Collection<Object>, Correspondence<MatchableTableRow,MatchableTableKey>, Correspondence<MatchableTableRow,MatchableTableKey>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Correspondence<MatchableTableRow, MatchableTableKey> record,
					DatasetIterator<Pair<Collection<Object>, Correspondence<MatchableTableRow, MatchableTableKey>>> resultCollector) {
				
				if(record.getFirstRecord().getTableId()>record.getSecondRecord().getTableId()) {
					System.out.println("Wrong direction!");
				}
				
				for(Correspondence<MatchableTableKey, MatchableTableRow> keyCor : record.getCausalCorrespondences().get()) {
					List<Integer> leftKey = Q.sort(Q.project(keyCor.getFirstRecord().getColumns(), new MatchableTableColumn.ColumnIndexProjection()));
					List<Integer> rightKey = Q.sort(Q.project(keyCor.getSecondRecord().getColumns(), new MatchableTableColumn.ColumnIndexProjection()));
					
					List<Object> keyValues = Q.toList(record.getFirstRecord().getTableId(), record.getSecondRecord().getTableId(), leftKey, rightKey);
					
//					System.out.println(String.format("[key] %s <-> %s", keyCor.getFirstRecord().getColumns(), keyCor.getSecondRecord().getColumns()));
//					System.out.println(String.format("%s  <->  %s", StringUtils.join(record.getFirstRecord().get(Q.toPrimitiveIntArray(leftKey)),","), StringUtils.join(record.getSecondRecord().get(Q.toPrimitiveIntArray(rightKey)),",")));
					
					resultCollector.next(new Pair<Collection<Object>, Correspondence<MatchableTableRow,MatchableTableKey>>(keyValues, record));
				}
			}
		};
		
		// group by table and key combination
		ResultSet<Group<Collection<Object>, Correspondence<MatchableTableRow, MatchableTableKey>>> grouped = proc.groupRecords(correspondences, groupByTablesMapper);
		
		
		// aggregate votes
		RecordMapper<Group<Collection<Object>, Correspondence<MatchableTableRow, MatchableTableKey>>, Correspondence<MatchableTableColumn, MatchableTableRow>> voteMapper = new RecordMapper<Group<Collection<Object>,Correspondence<MatchableTableRow,MatchableTableKey>>, Correspondence<MatchableTableColumn,MatchableTableRow>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Group<Collection<Object>, Correspondence<MatchableTableRow, MatchableTableKey>> record,
					DatasetIterator<Correspondence<MatchableTableColumn, MatchableTableRow>> resultCollector) {
		
				SimilarityMatrix<MatchableTableColumn> m = new SparseSimilarityMatrixFactory().createSimilarityMatrix(0, 0);
				
				// we have to transform the type of record correspondences
				ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> causes = new ResultSet<>();
				
				// each instance correspondence votes
				for(Correspondence<MatchableTableRow, MatchableTableKey> cor : record.getRecords().get()) {
					
					Map<MatchableTableColumn, MatchableTableColumn> mapping = new HashMap<>();
					
					//TODO this part could be a matching rule and not a part of the aggregator
					// let the correspondence vote
					//TODO this part contains an error: by re-matching the values, we get correspondences which we already excluded before ... ???
					//TODO the problem is that the 1:1 mapping is random if the scores are equal
					//TODO but the record links were created with a given mapping, which we should not change here!??
					Collection<Pair<MatchableTableColumn, MatchableTableColumn>> votes = matchColumns(cor.getFirstRecord(), cor.getSecondRecord());
					for(Pair<MatchableTableColumn, MatchableTableColumn> vote : votes) {
						m.add(vote.getFirst(), vote.getSecond(), 1.0);
						
						mapping.put(vote.getFirst(), vote.getSecond());
					}

					//TODO this might duplicate the causes, better only add the key from this records grouping key as cause
					ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> voteCauses = new ResultSet<>();
					for(Correspondence<MatchableTableKey, MatchableTableRow> keyCor : cor.getCausalCorrespondences().get()) {
						for(MatchableTableColumn c : keyCor.getFirstRecord().getColumns()) {
							Correspondence<MatchableTableColumn, MatchableTableRow> voteCauseCor = new Correspondence<MatchableTableColumn, MatchableTableRow>(c, mapping.get(c), 1.0, null);
							voteCauses.add(voteCauseCor);
						}
					}
					
					// transform the correspondence type
					Correspondence<MatchableTableRow, MatchableTableColumn> transformed = new Correspondence<MatchableTableRow, MatchableTableColumn>(cor.getFirstRecord(), cor.getSecondRecord(), cor.getSimilarityScore(), voteCauses);
					causes.add(transformed);
					
				}
				
				List<Object> groupingKey = new ArrayList<>(record.getKey()); 
//				System.out.println(String.format("Schema votes for #%d <-> #%d\n\tbased on %s <-> %s", groupingKey.get(0), groupingKey.get(1), groupingKey.get(2), groupingKey.get(3)));
//				System.out.println(m.getOutput());
//				if((int)groupingKey.get(0)==5 && (int)groupingKey.get(1)==9) {
//					for(Correspondence<MatchableTableRow, MatchableTableKey> cor : record.getRecords().get()) {
//						System.out.println(String.format("\t{#%d}[#%d]\t%s", cor.getFirstRecord().getTableId(), cor.getFirstRecord().getRowNumber(), cor.getFirstRecord().format(20)));
//						System.out.println(String.format("\t{#%d}[#%d]\t%s", cor.getSecondRecord().getTableId(), cor.getSecondRecord().getRowNumber(), cor.getSecondRecord().format(20)));
//					}
//				}
				
				//consider minimum number of votes
				if(minVotes>0) {
					int before = m.getNumberOfNonZeroElements();
					m.prune((double)minVotes);
					int removed = before - m.getNumberOfNonZeroElements();
					
					System.out.println(String.format("[MinVotes] removed %d correspondences", removed));
				}
				
				int keySize = ((List) (new ArrayList<>(record.getKey()).get(2))).size(); // not nice ...
				
				System.out.println("Votes");
				System.out.println(m.getOutput(100));
				
				if(!noFiltering) {
					// based on the votes, decide on a final schema mapping
					BestChoiceMatching bcm = new BestChoiceMatching();
					m = bcm.match(m);
				}
				
				if(mustIncludeDependant && m.getNumberOfNonZeroElements() == keySize) {
					// only the columns in the key could be mapped!
					return;
				}
				
//				SimilarityMatrix<MatchableTableColumn> counts = m.copy();
				
//				System.out.println(m.getOutput());
				
				SimilarityMatrix<MatchableTableColumn> counts = m.copy();
				m.normalize(record.getRecords().size());
				
				if(!uncertainOnly) {
//					m.prune(1.0);
					m.prune(minSimilarity);
				}
				
				// and output the correspondences
				for(MatchableTableColumn c1 : m.getFirstDimension()) {
					for(MatchableTableColumn c2 : m.getMatches(c1)) {
						
						Correspondence<MatchableTableColumn, MatchableTableRow> cor = new Correspondence<MatchableTableColumn, MatchableTableRow>(c1, c2, counts.get(c1, c2), causes);
//						Correspondence<MatchableTableColumn, MatchableTableRow> cor = new Correspondence<MatchableTableColumn, MatchableTableRow>(c1, c2, keySize, causes);
						
						if(!uncertainOnly || m.get(c1, c2)<1.0) {
//							if(uncertainOnly) {
//								System.out.println(counts.getOutput());
//								System.out.println(String.format("[uncertain] %s<->%s [%.4f]", c1,c2,m.get(c1, c2)));
//							}
							
							resultCollector.next(cor);
						}
					}
				}
			}
		};
		
		// just for nicer output in debugging
//		grouped = proc.sort(grouped, new Function<String, Group<Collection<Object>, Correspondence<MatchableTableRow, MatchableTableKey>>>() {
//
//			/**
//			 * 
//			 */
//			private static final long serialVersionUID = 1L;
//
//			@Override
//			public String execute(
//					Group<Collection<Object>, Correspondence<MatchableTableRow, MatchableTableKey>> input) {
//				return input.getKey().toString();
//			}
//		});
		
		return proc.transform(grouped, voteMapper);
		
	}
	
	public Collection<Pair<MatchableTableColumn, MatchableTableColumn>> matchColumns(MatchableTableRow record1, MatchableTableRow record2) {
		
		Collection<Pair<MatchableTableColumn, MatchableTableColumn>> matchingColumns = new LinkedList<>();

			for(MatchableTableColumn col : record1.getSchema()) {
				
//				if(!SpecialColumns.isSpecialColumn(col)) {
				
					Object value = record1.get(col.getColumnIndex());
					
					if(value==null) {
						value = "null";
					}
					
					for(MatchableTableColumn col2 : record2.getSchema()) {
						
//						if(!SpecialColumns.isSpecialColumn(col2)) {
							Object value2 = record2.get(col2.getColumnIndex());
							
							if(value2==null) {
								value2 = "null";
							}
							
							if(value.equals(value2)) {
								matchingColumns.add(new Pair<MatchableTableColumn, MatchableTableColumn>(col, col2));
							}
//						}
					}
//				}
			}
		
		return matchingColumns;
	}

}
