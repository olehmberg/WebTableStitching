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

import java.util.HashMap;
import java.util.Map;

import de.uni_mannheim.informatik.dws.t2k.utils.data.MapUtils2;
import de.uni_mannheim.informatik.wdi.matrices.SimilarityMatrix;
import de.uni_mannheim.informatik.wdi.matrices.SparseSimilarityMatrixFactory;
import de.uni_mannheim.informatik.wdi.matrices.matcher.BestChoiceMatching;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DatasetIterator;
import de.uni_mannheim.informatik.wdi.processing.Function;
import de.uni_mannheim.informatik.wdi.processing.Group;
import de.uni_mannheim.informatik.wdi.processing.RecordKeyValueMapper;
import de.uni_mannheim.informatik.wdi.processing.RecordMapper;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class BestChoiceGroupingMatcher<GroupingType, InType1 extends Comparable<InType1>, InType2> extends GroupingMatcher<InType1, InType2, InType1, InType2, GroupingType> {
	
	//TODO change design: it's not a good choice to define the functions in the constructor (cannot access fields of the class)
	
	/**
	 * @param groupByMapper
	 * @param transformationMapper
	 */
	public BestChoiceGroupingMatcher(final Function<GroupingType, Correspondence<InType1, InType2>> groupingKey) {
		super(new RecordKeyValueMapper<GroupingType, Correspondence<InType1,InType2>, Correspondence<InType1,InType2>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Correspondence<InType1, InType2> record,
					DatasetIterator<Pair<GroupingType, Correspondence<InType1, InType2>>> resultCollector) {
				
				// group by by grouping key
				GroupingType group = groupingKey.execute(record);
				
				resultCollector.next(new Pair<GroupingType, Correspondence<InType1,InType2>>(group, record));
				
			}
		}, new RecordMapper<Group<GroupingType,Correspondence<InType1,InType2>>, Correspondence<InType1,InType2>>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Group<GroupingType, Correspondence<InType1, InType2>> record,
					DatasetIterator<Correspondence<InType1, InType2>> resultCollector) {
				
				// create a similarity matrix
				SimilarityMatrix<InType1> m = SimilarityMatrix.fromCorrespondences(record.getRecords().get(), new SparseSimilarityMatrixFactory());
				
				// index the causal correspondences for later
				Map<InType1, Map<InType1, ResultSet<Correspondence<InType2, InType1>>>> causalCorrespondences = new HashMap<>();
				for(Correspondence<InType1, InType2> cor : record.getRecords().get()) {
					MapUtils2.put(causalCorrespondences, cor.getFirstRecord(), cor.getSecondRecord(), cor.getCausalCorrespondences());
				}
				
				// apply best-choice matcher
				BestChoiceMatching bcm = new BestChoiceMatching();
				m = bcm.match(m);
				
				// create output correspondences
				for(Correspondence<InType1, InType2> cor : m.<InType2>toCorrespondences()) {
					
					// re-set the causal correspondences (which were lost as SimilarityMatrix cannot store them)
					cor.setCausalCorrespondences(MapUtils2.get(causalCorrespondences, cor.getFirstRecord(), cor.getSecondRecord()));
					
					resultCollector.next(cor);
				}
			}
		});
	}

	
}
