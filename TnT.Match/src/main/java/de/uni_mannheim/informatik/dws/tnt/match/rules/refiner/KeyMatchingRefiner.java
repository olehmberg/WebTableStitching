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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

import de.uni_mannheim.informatik.dws.t2k.utils.query.Func;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableKey;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.wdi.matrices.ArrayBasedSimilarityMatrixFactory;
import de.uni_mannheim.informatik.wdi.matrices.SimilarityMatrix;
import de.uni_mannheim.informatik.wdi.matrices.SparseSimilarityMatrixFactory;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;
import de.uni_mannheim.informatik.wdi.processing.DatasetIterator;
import de.uni_mannheim.informatik.wdi.processing.RecordMapper;

/**
 * 
 * Matches two records if the key values of one record can be found in the other record
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class KeyMatchingRefiner {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private double minSimilarity = 0.0;
	
	/**
	 * 
	 */
	public KeyMatchingRefiner(double minSimilarity) {
		this.minSimilarity = minSimilarity;
	}
	

	public ResultSet<Correspondence<MatchableTableRow, MatchableTableKey>> run(ResultSet<Correspondence<MatchableTableRow, MatchableTableKey>> correspondences, DataProcessingEngine proc) {
	
		RecordMapper<Correspondence<MatchableTableRow, MatchableTableKey>, Correspondence<MatchableTableRow, MatchableTableKey>> mapper = new RecordMapper<Correspondence<MatchableTableRow,MatchableTableKey>, Correspondence<MatchableTableRow,MatchableTableKey>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Correspondence<MatchableTableRow, MatchableTableKey> record,
					DatasetIterator<Correspondence<MatchableTableRow, MatchableTableKey>> resultCollector) {
				
				Correspondence<MatchableTableRow, MatchableTableKey> cor = apply(record);
				
				if(cor!=null) {
					resultCollector.next(cor);
				}
				
			}
		};
		
		return proc.transform(correspondences, mapper);
		
	}
	
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.matching.MatchingRule#apply(java.lang.Object, java.lang.Object, de.uni_mannheim.informatik.wdi.model.ResultSet)
	 */
	public Correspondence<MatchableTableRow, MatchableTableKey> apply(Correspondence<MatchableTableRow, MatchableTableKey> correspondence) {

		MatchableTableRow record1 = correspondence.getFirstRecord();
		MatchableTableRow record2 = correspondence.getSecondRecord();
		
		SimilarityMatrix<MatchableTableKey> matchingValues = new ArrayBasedSimilarityMatrixFactory().createSimilarityMatrix(record1.getSchema().length, record2.getSchema().length);
		Collection<Pair<MatchableTableKey, MatchableTableKey>> matchingColumns1 = matchKeys(record1, record2, matchingValues);
		Collection<Pair<MatchableTableKey, MatchableTableKey>> matchingColumns2 = matchKeys(record2, record1, matchingValues);
		
		ResultSet<Correspondence<MatchableTableKey, MatchableTableRow>> keyCors = new ResultSet<>();
		
		// Similarity Value: if both sides of the correspondence are or contain a candidate key, we can trust it (1.0), otherwise, we might have found only a partial overlap in the attributes (<1.0)
		
		// in the direction from record1 -> record2, the left-hand side is always a candidate key, so we have to check the right-hand side
		Set<Set<MatchableTableColumn>> keys = new HashSet<>();
		for(MatchableTableColumn[] key : record2.getKeys()) {
			keys.add(Q.toSet(key));
		}
		for(final Pair<MatchableTableKey, MatchableTableKey> match : matchingColumns1) {
			
			Set<MatchableTableColumn> bestKeyMatch = Q.max(keys, new Func<Integer, Set<MatchableTableColumn>>(){

				@Override
				public Integer invoke(Set<MatchableTableColumn> in) {
					return Q.intersection(match.getSecond().getColumns(), in).size();
				}});
			
			int intersection = Q.intersection(match.getSecond().getColumns(), bestKeyMatch).size();
			
			double similarity = (double)intersection / (double)bestKeyMatch.size();
			
			if(similarity>=minSimilarity) {
				keyCors.add(new Correspondence<MatchableTableKey, MatchableTableRow>(match.getFirst(), match.getSecond(), similarity, null));
			}
		}
		
		// in the direction from recrod2 -> record1, the right-hand side is always a candidate key, so we have to check the left-hand side
		keys.clear();
		for(MatchableTableColumn[] key : record1.getKeys()) {
			keys.add(Q.toSet(key));
		}
		for(final Pair<MatchableTableKey, MatchableTableKey> match : matchingColumns2) {
			
			Set<MatchableTableColumn> bestKeyMatch = Q.max(keys, new Func<Integer, Set<MatchableTableColumn>>(){

				@Override
				public Integer invoke(Set<MatchableTableColumn> in) {
					return Q.intersection(match.getFirst().getColumns(), in).size();
				}});
			
			int intersection = Q.intersection(match.getFirst().getColumns(), bestKeyMatch).size();
			
			double similarity = (double)intersection / (double)bestKeyMatch.size();
			
			if(similarity>=minSimilarity) {
				keyCors.add(new Correspondence<MatchableTableKey, MatchableTableRow>(match.getSecond(), match.getFirst(), similarity, null));
			}
		}
		
		keyCors.deduplicate();
		
		if(keyCors.size()>0) { 
			return new Correspondence<MatchableTableRow, MatchableTableKey>(record1, record2, correspondence.getSimilarityScore(), keyCors);
		} else {
			return null;
		}
	}
		
	public Collection<Pair<MatchableTableKey, MatchableTableKey>> matchKeys(MatchableTableRow record1, MatchableTableRow record2, SimilarityMatrix<MatchableTableKey> matchingValues) {
		
		//TODO we have to consider all possible combinations here for the case of ambiguous key values
		
		Collection<Pair<MatchableTableKey, MatchableTableKey>> matchingColumns = new LinkedList<>();
		
		SimilarityMatrix<MatchableTableColumn> sim = new SparseSimilarityMatrixFactory().createSimilarityMatrix(0, 0);
		
		for(MatchableTableColumn[] key : record1.getKeys()) {
			
//			Set<MatchableTableColumn> matches = new HashSet<>();
			
			Set<MatchableTableColumn> schema2 = Q.toSet(record2.getSchema());
			
			for(MatchableTableColumn col : key) {
//				boolean hasMatch = false;
				Object value = record1.get(col.getColumnIndex());
				
				if(value==null) {
					value = "null";
				}
				
				Iterator<MatchableTableColumn> s2It = schema2.iterator();
				//for(MatchableTableColumn col2 : schema2) {
				while(s2It.hasNext()) {
					MatchableTableColumn col2 = s2It.next();
					
					Object value2 = record2.get(col2.getColumnIndex());
					
					if(value2==null) {
						value2 = "null";
					}
					
					if(value.equals(value2)) {
						sim.add(col, col2, 1.0);
						
//						matches.add(col2);
//						s2It.remove(); // remove the column so it cannot be mapped twice for the same key
//						hasMatch = true;
//						break;
					}
				}
//				
//				if(!hasMatch) {
//					// we are only interested in matching values if a full key matches
//					matches.clear();
//					break;
//				}
			}
//			
//			if(matches.size()==key.length) {
//				matchingColumns.add(new Pair<MatchableTableKey, MatchableTableKey>(new MatchableTableKey(record1.getTableId(), Q.toSet(key)), new MatchableTableKey(record2.getTableId(), matches)));
//			}
		}

		for(MatchableTableColumn[] key : record1.getKeys()) {

			Collection<Set<Pair<MatchableTableColumn,MatchableTableColumn>>> matchedColumns = addMatches(key, 0, sim, new HashSet<Pair<MatchableTableColumn,MatchableTableColumn>>());

			for(Set<Pair<MatchableTableColumn,MatchableTableColumn>> match : matchedColumns) {
				Set<MatchableTableColumn> key1 = new HashSet<>();
				Set<MatchableTableColumn> key2 = new HashSet<>();
				
				for(Pair<MatchableTableColumn,MatchableTableColumn> p : match) {
					key1.add(p.getFirst());
					key2.add(p.getSecond());
				}
				
				if(key1.size()==key2.size()) {
					matchingColumns.add(new Pair<MatchableTableKey, MatchableTableKey>(new MatchableTableKey(record1.getTableId(), key1), new MatchableTableKey(record2.getTableId(), key2)));
				}
			}
					
		}
		
		return matchingColumns;
	}

	private Collection<Set<Pair<MatchableTableColumn,MatchableTableColumn>>> addMatches(MatchableTableColumn[] key, int index, SimilarityMatrix<MatchableTableColumn> sim, Set<Pair<MatchableTableColumn,MatchableTableColumn>> matchedPart) {
		if(index<key.length) {
			
			Set<Set<Pair<MatchableTableColumn,MatchableTableColumn>>> results = new HashSet<>();
			
			MatchableTableColumn col1 = key[index];
			
			for(MatchableTableColumn col2 : sim.getMatches(col1)) {
	
				HashSet<Pair<MatchableTableColumn,MatchableTableColumn>> nextPart = new HashSet<>(matchedPart);
				
				nextPart.add(new Pair<>(col1,col2));
				
				results.addAll(addMatches(key, index+1, sim, nextPart));
				
			}
			
			return results;
		} else {
			return Q.toSet(matchedPart);
		}
	}

}
