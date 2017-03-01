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

import de.uni_mannheim.informatik.dws.tnt.match.DisjointHeaders;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.wdi.matching.MatchingRule;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.DefaultRecord;
import de.uni_mannheim.informatik.wdi.model.FeatureVectorDataSet;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;
import de.uni_mannheim.informatik.wdi.processing.DatasetIterator;
import de.uni_mannheim.informatik.wdi.processing.RecordMapper;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class DisjointHeaderSchemaMatchingRule  {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	/**
	 * @param finalThreshold
	 */
	public DisjointHeaderSchemaMatchingRule(DisjointHeaders disjointHeaders) {
		this.disjointHeaders = disjointHeaders;
	}

	private DisjointHeaders disjointHeaders;

	public ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> run(ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> correspondences, DataProcessingEngine proc) {
		
		RecordMapper<Correspondence<MatchableTableColumn, MatchableTableRow>, Correspondence<MatchableTableColumn, MatchableTableRow>> mapper = new RecordMapper<Correspondence<MatchableTableColumn, MatchableTableRow>, Correspondence<MatchableTableColumn, MatchableTableRow>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Correspondence<MatchableTableColumn, MatchableTableRow> record,
					DatasetIterator<Correspondence<MatchableTableColumn, MatchableTableRow>> resultCollector) {
				
				Correspondence<MatchableTableColumn, MatchableTableRow> cor = apply(record);
				
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
	public Correspondence<MatchableTableColumn, MatchableTableRow> apply(Correspondence<MatchableTableColumn, MatchableTableRow> correspondence) {
		
		String h1 = correspondence.getFirstRecord().getHeader();
		String h2 = correspondence.getSecondRecord().getHeader();
		
		if(!disjointHeaders.getDisjointHeaders(h1).contains(h2)
				&& !disjointHeaders.getDisjointHeaders(h2).contains(h1)) {
			return correspondence;
		} else {
			return null;
		}
		
	}

}
