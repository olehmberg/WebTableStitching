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
package de.uni_mannheim.informatik.dws.tnt.match.schemamatching;

import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumnValueGenerator;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.matching.aggregators.VotingAggregator;
import de.uni_mannheim.informatik.dws.winter.matching.algorithms.SymmetricInstanceBasedSchemaMatchingAlgorithm;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.InstanceBasedSchemaBlocker;
import de.uni_mannheim.informatik.dws.winter.matrices.SimilarityMatrix;
import de.uni_mannheim.informatik.dws.winter.matrices.SparseSimilarityMatrixFactory;
import de.uni_mannheim.informatik.dws.winter.matrices.matcher.BestChoiceMatching;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.MatchableValue;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Group;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.RecordKeyValueMapper;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class ValueBasedSchemaMatcher {

	public Processable<Correspondence<MatchableTableColumn, Matchable>> run(DataSet<MatchableTableRow, MatchableTableColumn> records) {
		
		InstanceBasedSchemaBlocker<MatchableTableRow, MatchableTableColumn> blocker = new InstanceBasedSchemaBlocker<>(new MatchableTableColumnValueGenerator());
		VotingAggregator<MatchableTableColumn, MatchableValue> aggregator = new VotingAggregator<>(false, 0.0);
		
		SymmetricInstanceBasedSchemaMatchingAlgorithm<MatchableTableRow, MatchableTableColumn> matcher = new SymmetricInstanceBasedSchemaMatchingAlgorithm<MatchableTableRow, MatchableTableColumn>(records, blocker, aggregator);
		
		matcher.run();
		
		Processable<Correspondence<MatchableTableColumn, MatchableValue>> schemaCorrespondences = matcher.getResult();
		
		// create a 1:1 mapping of the attributes
		
		Processable<Group<Pair<Integer, Integer>, Correspondence<MatchableTableColumn, MatchableValue>>> groupedByTables = schemaCorrespondences.group(new RecordKeyValueMapper<Pair<Integer, Integer>, Correspondence<MatchableTableColumn,MatchableValue>, Correspondence<MatchableTableColumn,MatchableValue>>() {

			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecordToKey(Correspondence<MatchableTableColumn, MatchableValue> record,
					DataIterator<Pair<Pair<Integer, Integer>, Correspondence<MatchableTableColumn, MatchableValue>>> resultCollector) {
		
				resultCollector.next(
						new Pair<Pair<Integer,Integer>, Correspondence<MatchableTableColumn,MatchableValue>>(
								new Pair<>(record.getFirstRecord().getDataSourceIdentifier(), record.getSecondRecord().getDataSourceIdentifier()),
								record));
				
			}
		});
		
		Processable<Correspondence<MatchableTableColumn, Matchable>> top1Correspondences = groupedByTables.map((r,c) -> {
		
			SimilarityMatrix<MatchableTableColumn> m = SimilarityMatrix.fromCorrespondences(r.getRecords().get(), new SparseSimilarityMatrixFactory());
			BestChoiceMatching b = new BestChoiceMatching();
			
			m = b.match(m);
			
			for(MatchableTableColumn c1 : m.getFirstDimension()) {
				for(MatchableTableColumn c2 : m.getMatches(c1)) {
					
					if(m.get(c1, c2)>0.0) {
						Correspondence<MatchableTableColumn, Matchable> cor = new Correspondence<MatchableTableColumn, Matchable>(c1, c2, m.get(c1, c2), null);
						
						c.next(cor);
					}
					
				}
			}
		});
		
		
		
//		MatchingEngine<MatchableTableColumn, Matchable> engine = new MatchingEngine<>();
//		Processable<Correspondence<MatchableTableColumn, Matchable>> top1Correspondences = engine.getTopKInstanceCorrespondences(Correspondence.toMatchable(schemaCorrespondences), 1, 0.0);
		
		Correspondence.setDirectionByDataSourceIdentifier(top1Correspondences);
		
		return top1Correspondences;
	}
	
}
