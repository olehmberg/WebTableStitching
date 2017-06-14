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
package de.uni_mannheim.informatik.dws.tnt.match.stitching;

import java.util.Collection;
import java.util.Map;

import de.uni_mannheim.informatik.dws.tnt.match.ContextColumns;
import de.uni_mannheim.informatik.dws.tnt.match.TableReconstructor;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableDeterminant;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.KeyMappedCorrespondenceFilter;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class StitchedUnionTables {

	public Collection<Table> create(
			Map<Integer, Table> tables,
			Processable<MatchableTableRow> records,
			Processable<MatchableTableColumn> attributes, 
			DataSet<MatchableTableDeterminant, MatchableTableColumn> candidateKeys,
			Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences) throws Exception {
		/*********************************************** 
    	 * merge the tables
    	 ***********************************************/
    	TableReconstructor tr = new TableReconstructor(tables);
    	
    	// filter out correspondences between tables where no key is mapped and re-construct multiple tables
    	KeyMappedCorrespondenceFilter keyFilter = new KeyMappedCorrespondenceFilter(true);
    	schemaCorrespondences = keyFilter.runBlocking(candidateKeys, true, schemaCorrespondences);
    	
    	// at this point, there are schema correspondences between two tables only if at least one candidate key can be completely mapped
    	// so tables which are not connected (but might be connected via another table) will definitely produce a denormalised table as result
    	

    	Collection<Table> reconstructed = tr.reconstruct(0, records, attributes, candidateKeys, schemaCorrespondences);
    	for(Table t : reconstructed) {
    		// remove sparse columns: less than 5% non-empty
    		Table dense = tr.removeSparseColumns(t, 0.05);
    		
    		// identify a new entity label column
    		dense.identifySubjectColumn(0.001, true);
    		
    		// don't use context columns as keys (if a context column was chosen, remove the entity label column)
    		if(dense.hasSubjectColumn()) {
    			if(ContextColumns.isContextColumn(dense.getSubjectColumn())) {
    				dense.setSubjectColumnIndex(-1);
    			}
    		}
    	}
    
    	return reconstructed;
	}
	
}
