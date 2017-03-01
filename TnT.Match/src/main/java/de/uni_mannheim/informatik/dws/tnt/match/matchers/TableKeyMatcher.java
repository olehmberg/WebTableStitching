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
package de.uni_mannheim.informatik.dws.tnt.match.matchers;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableKey;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.wdi.matrices.ArrayBasedSimilarityMatrixFactory;
import de.uni_mannheim.informatik.wdi.matrices.SimilarityMatrix;
import de.uni_mannheim.informatik.wdi.matrices.SparseSimilarityMatrixFactory;
import de.uni_mannheim.informatik.wdi.model.Correspondence;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class TableKeyMatcher {

	public SimilarityMatrix<MatchableTableKey> matchKeys(Collection<Correspondence<MatchableTableRow, MatchableTableColumn>> correspondences) {
		SimilarityMatrix<MatchableTableKey> keyCombinations = new SparseSimilarityMatrixFactory().createSimilarityMatrix(0, 0);
		
		for(Correspondence<MatchableTableRow, MatchableTableColumn> cor : correspondences) {
			SimilarityMatrix<MatchableTableColumn> columMatches = SimilarityMatrix.fromCorrespondences(cor.getCausalCorrespondences().get(), new ArrayBasedSimilarityMatrixFactory());
			
			MatchableTableRow row1 = cor.getFirstRecord();
			MatchableTableRow row2 = cor.getSecondRecord();
			
			// determine which keys are matched between the two records
			for(MatchableTableColumn[] key1 : row1.getKeys()) {
				
				// make sure there is any mapping for key1
				if(Q.all(Q.toList(key1), columMatches.getHasMatchPredicate())) {
				
					for(MatchableTableColumn[] key2 : row2.getKeys()) {
						// check if there is any mapping to key2
						
						List<MatchableTableColumn> key1Columns = new LinkedList<>(Q.toList(key1));
						
						for(MatchableTableColumn c2 : key2) {
							Iterator<MatchableTableColumn> key1It = key1Columns.iterator();
							while(key1It.hasNext()) {
								if(columMatches.get(key1It.next(), c2)!=null) {
									key1It.remove();
								}
							}
						}
						
						if(key1Columns.size()==0) {
							// all columns have a match
							// so we have found a matching key combination
							MatchableTableKey k1 = new MatchableTableKey(row1.getTableId(), Q.toSet(key1));
							MatchableTableKey k2 = new MatchableTableKey(row2.getTableId(), Q.toSet(key2));
							Double last = keyCombinations.get(k1,k2);
							if(last==null) {
								last = 0.0;
							}
							keyCombinations.set(k1, k2, last+1.0);
							
						}
					}
					
				}
			}
		}
		
		return keyCombinations;
	}
	
}
