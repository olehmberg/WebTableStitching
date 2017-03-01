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
package de.uni_mannheim.informatik.dws.tnt.match.rules.graph;

import de.uni_mannheim.informatik.dws.tnt.match.DisjointHeaders;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableKey;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;
import de.uni_mannheim.informatik.wdi.processing.DatasetIterator;
import de.uni_mannheim.informatik.wdi.processing.RecordMapper;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class FindConflictsByMultiLinkage {

	public ResultSet<Correspondence<MatchableTableRow, MatchableTableKey>> match(
			ResultSet<Correspondence<MatchableTableRow, MatchableTableKey>> schemaCorrespondences, 
			DataProcessingEngine proc,
			DisjointHeaders dh) {
		
		// get a list of matched records
		RecordMapper<Correspondence<MatchableTableRow, MatchableTableKey>, MatchableTableRow> getRecords = new RecordMapper<Correspondence<MatchableTableRow, MatchableTableKey>, MatchableTableRow>() {
			
			@Override
			public void mapRecord(Correspondence<MatchableTableRow, MatchableTableKey> record,
					DatasetIterator<MatchableTableRow> resultCollector) {
				resultCollector.next(record.getFirstRecord());
				resultCollector.next(record.getSecondRecord());
			}
		};
		ResultSet<MatchableTableRow> records = proc.transform(schemaCorrespondences, getRecords);
		
		//TODO graph of record links (records as nodes!)
		
		return schemaCorrespondences;
		
	}
	
}
