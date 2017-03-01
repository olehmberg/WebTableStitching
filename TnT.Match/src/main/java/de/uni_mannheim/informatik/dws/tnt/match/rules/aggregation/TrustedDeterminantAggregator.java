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

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import de.uni_mannheim.informatik.dws.t2k.utils.query.Func;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.t2k.webtables.Table;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableRow;
import de.uni_mannheim.informatik.dws.t2k.webtables.writers.CSVTableWriter;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableKey;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.match.dependencies.FunctionalDependencyUtils;
import de.uni_mannheim.informatik.wdi.matrices.SimilarityMatrix;
import de.uni_mannheim.informatik.wdi.matrices.SparseSimilarityMatrixFactory;
import de.uni_mannheim.informatik.wdi.matrices.matcher.BestChoiceMatching;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;
import de.uni_mannheim.informatik.wdi.processing.DatasetIterator;
import de.uni_mannheim.informatik.wdi.processing.Group;
import de.uni_mannheim.informatik.wdi.processing.RecordKeyValueMapper;
import de.uni_mannheim.informatik.wdi.processing.RecordMapper;

/**
 * 
 * Determines all combinations of candidate keys that are possible for the record links and considers the most frequent as "trusted".
 * 
 * Example: Key Matching Matrix
 * 			KeyB1	KeyB2
 * keyA1	1		3
 * KeyA2	3		0
 * 
 * Key combinations KeyA1<->KeyB2 and KeyA2<->KeyB1 will be trusted, keyA1<->KeyB1 not.
 * 
 * Then, all record links which were not created by all trusted keys are removed
 * 
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class TrustedDeterminantAggregator {


	private int minKeyCorrespondences = 1;
	private int minRecordLinksPerKey = 1;
//	private File csvLocation;
	
	/**
	 * 
	 */
	public TrustedDeterminantAggregator(int minKeyCorrespondences) {
		this.minKeyCorrespondences = minKeyCorrespondences;
	}
	
	public ResultSet<Correspondence<MatchableTableRow, MatchableTableKey>> aggregate(ResultSet<Correspondence<MatchableTableRow, MatchableTableKey>> correspondences,
			DataProcessingEngine proc) {
		
		proc = new DataProcessingEngine();
		
		// Group all records by table
		RecordKeyValueMapper<Collection<Integer>, Correspondence<MatchableTableRow, MatchableTableKey>, Correspondence<MatchableTableRow, MatchableTableKey>> groupByTablesMapper = new RecordKeyValueMapper<Collection<Integer>, Correspondence<MatchableTableRow,MatchableTableKey>, Correspondence<MatchableTableRow,MatchableTableKey>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Correspondence<MatchableTableRow, MatchableTableKey> record,
					DatasetIterator<Pair<Collection<Integer>, Correspondence<MatchableTableRow, MatchableTableKey>>> resultCollector) {
				
				if(record.getFirstRecord().getTableId()>record.getSecondRecord().getTableId()) {
					System.out.println("Wrong direction!");
				}
				
				resultCollector.next(new Pair<Collection<Integer>, Correspondence<MatchableTableRow,MatchableTableKey>>(Q.toList(record.getFirstRecord().getTableId(), record.getSecondRecord().getTableId()), record));
			}
		};
		ResultSet<Group<Collection<Integer>, Correspondence<MatchableTableRow, MatchableTableKey>>> grouped = proc.groupRecords(correspondences, groupByTablesMapper);
		
		// remove the untrusted correspondences
		RecordMapper<Group<Collection<Integer>, Correspondence<MatchableTableRow, MatchableTableKey>>, Correspondence<MatchableTableRow, MatchableTableKey>> removeUntrustedCorrespondences = new RecordMapper<Group<Collection<Integer>,Correspondence<MatchableTableRow,MatchableTableKey>>, Correspondence<MatchableTableRow,MatchableTableKey>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Group<Collection<Integer>, Correspondence<MatchableTableRow, MatchableTableKey>> record,
					DatasetIterator<Correspondence<MatchableTableRow, MatchableTableKey>> resultCollector) {

				// combine all correspondences between the same records (with different keys)
				HashMap<List<Integer>, Correspondence<MatchableTableRow, MatchableTableKey>> correspondences = new HashMap<>();
				
				// count the key combinations in the instance correspondences
				SimilarityMatrix<MatchableTableKey> keyFrequencies = new SparseSimilarityMatrixFactory().createSimilarityMatrix(0, 0);
				
				for(Correspondence<MatchableTableRow, MatchableTableKey> cor : record.getRecords().get()) {
					List<Integer> rows = Q.toList(cor.getFirstRecord().getRowNumber(), cor.getSecondRecord().getRowNumber());
					Correspondence<MatchableTableRow, MatchableTableKey> merged = correspondences.get(rows);
					if(merged==null) {
						merged = new Correspondence<MatchableTableRow, MatchableTableKey>(cor.getFirstRecord(), cor.getSecondRecord(), cor.getSimilarityScore(), new ResultSet<Correspondence<MatchableTableKey, MatchableTableRow>>());
						correspondences.put(rows, merged);
					}
					
					// for each key correspondence that created this instance correspondence, increase the count for that key combination
					for(Correspondence<MatchableTableKey, MatchableTableRow> keyCor : cor.getCausalCorrespondences().get()) {
					
						merged.getCausalCorrespondences().add(keyCor);
						
						MatchableTableKey k1 = keyCor.getFirstRecord();
						MatchableTableKey k2 = keyCor.getSecondRecord();
						
						Double existing = keyFrequencies.get(k1, k2);
						if(existing==null) {
							existing=0.0;
						}
						
						keyFrequencies.set(k1, k2, existing+1.0);
					}
				}
				
//				if(record.getKey().contains(23) && record.getKey().contains(29)) {
//					System.out.println(keyFrequencies.getOutput(100));
//					
//					List<Integer> ids = new ArrayList<>(record.getKey());
//					
//					Table t1 = new Table();
//					t1.setTableId(ids.get(0));
//					t1.setPath(String.format("%d_%d__%d", ids.get(0), ids.get(1), ids.get(0)));
//					Table t2 = new Table();
//					t2.setTableId(ids.get(1));
//					t2.setPath(String.format("%d_%d__%d", ids.get(0), ids.get(1), ids.get(1)));
//					
//					int rowNumber = 0;
//					for(Correspondence<MatchableTableRow, MatchableTableKey> cor : record.getRecords().get()) {
//						
//						if(t1.getColumns().size()==0) {
//							int columnIndex = 0;
//							for(MatchableTableColumn c : cor.getFirstRecord().getSchema()) {
//								TableColumn col = new TableColumn(columnIndex, t1);
//								col.setHeader(c.getHeader());
//								t1.addColumn(col);
//								columnIndex++;
//							}
//						}
//						
//						TableRow r1 = new TableRow(rowNumber, t1);
//						r1.set(cor.getFirstRecord().getValues());
//						t1.addRow(r1);
//						
//						
//						if(t2.getColumns().size()==0) {
//							int columnIndex = 0;
//							for(MatchableTableColumn c : cor.getSecondRecord().getSchema()) {
//								TableColumn col = new TableColumn(columnIndex, t2);
//								col.setHeader(c.getHeader());
//								t2.addColumn(col);
//								columnIndex++;
//							}
//						}
//						
//						
//						TableRow r2 = new TableRow(rowNumber, t2);
//						r2.set(cor.getSecondRecord().getValues());
//						t2.addRow(r2);
//						
//						rowNumber++;
//					}
//					
//					CSVTableWriter tw = new CSVTableWriter();
//					
//					try {
//						
//						Table tWithoutContext = null;
//						File fWithoutContext = null;
//						Map<Collection<TableColumn>, Collection<TableColumn>> functionalDependenciesWithoutContext = null;
//						Map<Collection<TableColumn>, Collection<TableColumn>> fds = null;
//						if(t1.getRows().size()>0) {
//				    		// create a table without the context surrogate key and row number surrogate
//				    		tWithoutContext = t1.project(Q.where(t1.getColumns(), new Func<Boolean, TableColumn>() {
//		
//								@Override
//								public Boolean invoke(TableColumn in) {
//									return in.getColumnIndex() > 1;
//								}}));
//				    		
//				    		tw.write(t1, new File(csvLocation, t1.getPath()));
//				    		fWithoutContext = tw.write(tWithoutContext, new File(csvLocation, "nocontext_" + t1.getPath()));
//				    		functionalDependenciesWithoutContext = FunctionalDependencyUtils.calculateFunctionalDependencies(tWithoutContext, fWithoutContext);
//		
//				    		System.out.println("\tFunctional Dependencies without context");
//				    		for(Map.Entry<Collection<TableColumn>, Collection<TableColumn>> fd : functionalDependenciesWithoutContext.entrySet()) {
//				    			System.out.println(String.format("\t\t{%s}->{%s}", 
//				    					StringUtils.join(Q.project(fd.getKey(), new TableColumn.ColumnHeaderProjection()), ","), 
//				    					StringUtils.join(Q.project(fd.getValue(), new TableColumn.ColumnHeaderProjection()), ",")));
//				    		}
//				    		
//				    		// the functionalDependenciesWithoutContext are only valid for tWithoutContext!
//		//		    		t.getSchema().setFunctionalDependencies(functionalDependenciesWithoutContext);
//				    		// so we have to translate the column indices
//				    		fds = new HashMap<>();
//				    		for(Collection<TableColumn> detWC : functionalDependenciesWithoutContext.keySet()) {
//				    			Collection<TableColumn> det = new HashSet<>();
//				    			Collection<TableColumn> depWC = functionalDependenciesWithoutContext.get(detWC);
//				    			Collection<TableColumn> dep = new HashSet<>();
//				    			
//				    			for(TableColumn c : detWC) {
//				    				det.add(t1.getSchema().get(c.getColumnIndex()+2));
//				    			}
//				    			for(TableColumn c : depWC) {
//				    				dep.add(t1.getSchema().get(c.getColumnIndex()+2));
//				    			}
//				    			fds.put(det, dep);
//				    		}
//				    		t1.getSchema().setFunctionalDependencies(fds);
//						}
//					
//					if(t2.getRows().size()>0) {
//			    		tWithoutContext = t2.project(Q.where(t2.getColumns(), new Func<Boolean, TableColumn>() {
//	
//							@Override
//							public Boolean invoke(TableColumn in) {
//								return in.getColumnIndex() > 1;
//							}}));
//			    		tw.write(t2, new File(csvLocation, t2.getPath()));
//			    		fWithoutContext = tw.write(tWithoutContext, new File(csvLocation, "nocontext_" + t2.getPath()));
//			    		functionalDependenciesWithoutContext = FunctionalDependencyUtils.calculateFunctionalDependencies(tWithoutContext, fWithoutContext);
//	
//			    		System.out.println("\tFunctional Dependencies without context");
//			    		for(Map.Entry<Collection<TableColumn>, Collection<TableColumn>> fd : functionalDependenciesWithoutContext.entrySet()) {
//			    			System.out.println(String.format("\t\t{%s}->{%s}", 
//			    					StringUtils.join(Q.project(fd.getKey(), new TableColumn.ColumnHeaderProjection()), ","), 
//			    					StringUtils.join(Q.project(fd.getValue(), new TableColumn.ColumnHeaderProjection()), ",")));
//			    		}
//			    		
//			    		// the functionalDependenciesWithoutContext are only valid for tWithoutContext!
//	//		    		t.getSchema().setFunctionalDependencies(functionalDependenciesWithoutContext);
//			    		// so we have to translate the column indices
//			    		fds = new HashMap<>();
//			    		for(Collection<TableColumn> detWC : functionalDependenciesWithoutContext.keySet()) {
//			    			Collection<TableColumn> det = new HashSet<>();
//			    			Collection<TableColumn> depWC = functionalDependenciesWithoutContext.get(detWC);
//			    			Collection<TableColumn> dep = new HashSet<>();
//			    			
//			    			for(TableColumn c : detWC) {
//			    				det.add(t2.getSchema().get(c.getColumnIndex()+2));
//			    			}
//			    			for(TableColumn c : depWC) {
//			    				dep.add(t2.getSchema().get(c.getColumnIndex()+2));
//			    			}
//			    			fds.put(det, dep);
//			    		}
//			    		t2.getSchema().setFunctionalDependencies(fds);
//					}
//		    		
//					} catch(Exception ex) {
//						ex.printStackTrace();
//					}
//				}
				
				System.out.println(keyFrequencies.getOutput(100));
				
				// decide for the most frequent combinations
				BestChoiceMatching bcm = new BestChoiceMatching();
				SimilarityMatrix<MatchableTableKey> trustedKeys = bcm.match(keyFrequencies);
				
				System.out.println(trustedKeys.getOutput(100));
				
//				trustedKeys.normalize();
//				trustedKeys.prune(1.0);
				
				// now remove all correspondences that were created by determinants which are no longer in trustedKeys
				for(Correspondence<MatchableTableRow, MatchableTableKey> cor : record.getRecords().get()) {
					
					Collection<Correspondence<MatchableTableKey, MatchableTableRow>> validKeyCors = new ArrayList<>(cor.getCausalCorrespondences().size());
					
					for(Correspondence<MatchableTableKey, MatchableTableRow> keyCor : cor.getCausalCorrespondences().get()) {
						Double value = trustedKeys.get(keyCor.getFirstRecord(), keyCor.getSecondRecord());
						
						// the combination of determinants must be in the trustedKeys Matrix and be more frequent than minKeyCorrespondences (in terms of record links)
						if(value!=null && value >= minRecordLinksPerKey) {
							validKeyCors.add(keyCor);
						}
					}
					
					if(validKeyCors.size() >= minKeyCorrespondences) {
						cor.setCausalCorrespondences(new ResultSet<>(validKeyCors));
						resultCollector.next(cor);
					}
					
				}
				
				
//				// we need at least two trusted keys to be able to judge the correspondences
//				if(trustedKeys.getNumberOfNonZeroElements()>=minKeyCorrespondences) {
//					
//					// only output correspondences where all the frequent combinations matched
//					for(Correspondence<MatchableTableRow, MatchableTableKey> cor : correspondences.values()) {
//						
//						// check all key correspondences
//						Iterator<Correspondence<MatchableTableKey, MatchableTableRow>> keyIt = cor.getCausalCorrespondences().get().iterator();
//						while(keyIt.hasNext()) {
//							Correspondence<MatchableTableKey, MatchableTableRow> keyCor = keyIt.next();
//							
//							if(trustedKeys.get(keyCor.getFirstRecord(), keyCor.getSecondRecord())==null) {
//								// this is an untrusted key and hence removed
//								keyIt.remove();
//							}
//						}
//						
//						
//						//TODO checking the number of keys for a correspondence must take into account the determinants ... if we use subkey matches, not all matching keys must create all correct record links (a smaller key creates more record links than a larger key) 
//						// now only trusted key correspondences remain
//						// if the number of key correspondences does not match the number of trusted keys, the instance correspondence is untrusted and removed
//						if(cor.getCausalCorrespondences().size()==trustedKeys.getNumberOfNonZeroElements()) {
//							resultCollector.next(cor);
//						} else {
//							
//							if(record.getKey().equals(Arrays.asList(new Integer[] {23,29}))) {
//							
//							System.out.println(keyFrequencies.getOutput());
//							System.out.println(trustedKeys.getOutput());
//							
//							for(MatchableTableKey k1 : trustedKeys.getFirstDimension()) {
//								for(MatchableTableKey k2 : trustedKeys.getMatches(k1)) {
//									
//									boolean found = false;
//									for(Correspondence<MatchableTableKey, MatchableTableRow> keyCor : cor.getCausalCorrespondences().get()) {
//										if(keyCor.getFirstRecord().equals(k1) && keyCor.getSecondRecord().equals(k2)) {
//											found=true;
//											break;
//										}
//									}
//									
//									if(!found) {
//										System.out.println(String.format("-x {%s}<->{%s}", k1.getColumns(), k2.getColumns()));
//									}
//									
//								}
//							}
//							
//							System.out.println(String.format("-- %s", cor.getFirstRecord().format(20)));
//							System.out.println(String.format("-- %s", cor.getSecondRecord().format(20)));
//							
//							System.out.println();
//								
//							}
//						}
//					}
//				}
				
			}
		};
		
		return proc.transform(grouped, removeUntrustedCorrespondences);
		
	}
	
}
