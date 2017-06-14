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

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import de.uni_mannheim.informatik.dws.winter.clustering.ConnectedComponentClusterer;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.model.Triple;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Function;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.processing.RecordMapper;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;

/**
 * 
 * Materialises transitivity of correspondences.
 * 
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class CorrespondenceTransitivityMaterialiser {

	public 
	<T extends Matchable, U extends Matchable> 
	Processable<Correspondence<T,U>> 
	aggregate(Processable<Correspondence<T,U>> correspondences) {
		
		ConnectedComponentClusterer<T> clusterer = new ConnectedComponentClusterer<>();
		Set<T> nodes = new HashSet<>();
		for(Correspondence<T,U> cor : correspondences.get()) {
			clusterer.addEdge(new Triple<T, T, Double>(cor.getFirstRecord(), cor.getSecondRecord(), cor.getSimilarityScore()));
		}
		for(T n : nodes) {
			clusterer.addEdge(new Triple<T, T, Double>(n, n, 1.0));
		}
		Map<Collection<T>, T> clusters = clusterer.createResult();
		
		Processable<Correspondence<T,U>> all = new ProcessableCollection<>();
		for(Collection<T> clu : clusters.keySet()) {
			List<T> list = Q.sort(clu, 
					(T o1, T o2) -> 
			{
				int cmp = Integer.compare(o1.getDataSourceIdentifier(), o2.getDataSourceIdentifier());
				
				if(cmp==0) {
					cmp = o1.getIdentifier().compareTo(o2.getIdentifier());	
				}
				
				return cmp;
			});
			
			for(int i=0; i<list.size();i++) {
				for(int j=i+1; j<list.size(); j++) {
					Correspondence<T,U> cor = new Correspondence<T, U>(list.get(i), list.get(j), 1.0, null);
					all.add(cor);
				}
			}
		}

		Function<String, Correspondence<T, U>> joinKeyGenerator = new Function<String, Correspondence<T,U>>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public String execute(Correspondence<T, U> input) {
				return String.format("%s/%s", input.getFirstRecord().getIdentifier(), input.getSecondRecord().getIdentifier());
			}
		};
		Processable<Pair<Correspondence<T, U>, Correspondence<T, U>>> joined = all.leftJoin(correspondences, joinKeyGenerator, joinKeyGenerator);
		
		RecordMapper<Pair<Correspondence<T, U>, Correspondence<T, U>>, Correspondence<T, U>> transformation = new RecordMapper<Pair<Correspondence<T,U>,Correspondence<T,U>>, Correspondence<T,U>>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(
					Pair<Correspondence<T,U>, Correspondence<T,U>> record,
					DataIterator<Correspondence<T,U>> resultCollector) {
				if(record.getSecond()==null) {
					resultCollector.next(record.getFirst());
				}
			}
		};
		return joined.transform(transformation);
	}
	
}
