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

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.wdi.clustering.ConnectedComponentClusterer;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.model.Triple;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;
import de.uni_mannheim.informatik.wdi.processing.DatasetIterator;
import de.uni_mannheim.informatik.wdi.processing.Function;
import de.uni_mannheim.informatik.wdi.processing.RecordMapper;

/**
 * 
 * Materialises transitivity of correspondences.
 * 
 * exactly the same code can be used for instance correspondences by swapping MatchableTableColumn and MatchableTableRow
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class SchemaTransitivityAggregator {

	public ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> aggregate(ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> correspondences,
			DataProcessingEngine proc) {
		
		ConnectedComponentClusterer<MatchableTableColumn> clusterer = new ConnectedComponentClusterer<>();
		Set<MatchableTableColumn> nodes = new HashSet<>();
		for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : correspondences.get()) {
			clusterer.addEdge(new Triple<MatchableTableColumn, MatchableTableColumn, Double>(cor.getFirstRecord(), cor.getSecondRecord(), cor.getSimilarityScore()));
		}
		for(MatchableTableColumn col : nodes) {
			clusterer.addEdge(new Triple<MatchableTableColumn, MatchableTableColumn, Double>(col, col, 1.0));
		}
		Map<Collection<MatchableTableColumn>, MatchableTableColumn> clusters = clusterer.createResult();
		
		ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> all = new ResultSet<>();
		for(Collection<MatchableTableColumn> clu : clusters.keySet()) {
			List<MatchableTableColumn> list = Q.sort(clu, new MatchableTableColumn.TableIdColumnIndexComparator());
			
			for(int i=0; i<list.size();i++) {
				for(int j=i+1; j<list.size(); j++) {
					Correspondence<MatchableTableColumn, MatchableTableRow> cor = new Correspondence<MatchableTableColumn, MatchableTableRow>(list.get(i), list.get(j), 1.0, null);
					all.add(cor);
				}
			}
		}

		Function<String, Correspondence<MatchableTableColumn, MatchableTableRow>> joinKeyGenerator = new Function<String, Correspondence<MatchableTableColumn,MatchableTableRow>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public String execute(Correspondence<MatchableTableColumn, MatchableTableRow> input) {
				return String.format("%s/%s", input.getFirstRecord().getIdentifier(), input.getSecondRecord().getIdentifier());
			}
		};
		ResultSet<Pair<Correspondence<MatchableTableColumn, MatchableTableRow>, Correspondence<MatchableTableColumn, MatchableTableRow>>> joined = proc.leftJoin(all, correspondences, joinKeyGenerator, joinKeyGenerator);
		
		RecordMapper<Pair<Correspondence<MatchableTableColumn, MatchableTableRow>, Correspondence<MatchableTableColumn, MatchableTableRow>>, Correspondence<MatchableTableColumn, MatchableTableRow>> transformation = new RecordMapper<Pair<Correspondence<MatchableTableColumn,MatchableTableRow>,Correspondence<MatchableTableColumn,MatchableTableRow>>, Correspondence<MatchableTableColumn,MatchableTableRow>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(
					Pair<Correspondence<MatchableTableColumn, MatchableTableRow>, Correspondence<MatchableTableColumn, MatchableTableRow>> record,
					DatasetIterator<Correspondence<MatchableTableColumn, MatchableTableRow>> resultCollector) {
				if(record.getSecond()==null) {
					resultCollector.next(record.getFirst());
				}
			}
		};
		return proc.transform(joined, transformation);
	}
	
}
