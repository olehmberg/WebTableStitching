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
package de.uni_mannheim.informatik.dws.t2k.match.spark;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableLodColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.utils.StringUtils;
import de.uni_mannheim.informatik.dws.t2k.webtables.Table;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.t2k.webtables.lod.LodTableColumn;
import de.uni_mannheim.informatik.wdi.processing.DatasetIterator;
import de.uni_mannheim.informatik.wdi.processing.RecordMapper;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class LodTableSchemaLoader implements RecordMapper<Table, MatchableTableColumn> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private Map<Integer, Map<Integer, Integer>> propertyIndicesInverse;
	
	/**
	 * 
	 */
	public LodTableSchemaLoader(Map<Integer, Map<Integer, Integer>> propertyIndicesInverse) {
		this.propertyIndicesInverse = propertyIndicesInverse;
	}
	
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.processing.RecordMapper#mapRecord(java.lang.Object, de.uni_mannheim.informatik.wdi.processing.DatasetIterator)
	 */
	@Override
	public void mapRecord(Table record, DatasetIterator<MatchableTableColumn> resultCollector) {
		
		record.setKeyIndex(1);
		
		// remove object properties and keep only "_label" columns (otherwise we will have duplicate property URLs)
		LodTableColumn[] cols = record.getColumns().toArray(new LodTableColumn[record.getSchema().getSize()]);
		List<LodTableColumn> removedColumns = new LinkedList<>();
		for(LodTableColumn tc : cols) {
			if(tc.isReferenceLabel()) {
				Iterator<TableColumn> it = record.getSchema().getRecords().iterator();
				
				while(it.hasNext()) {
					LodTableColumn ltc = (LodTableColumn)it.next();
					
					if(!ltc.isReferenceLabel() && ltc.getUri().equals(tc.getUri())) {
						it.remove();
						removedColumns.add(ltc);
					}
				}
			}
		}
		
		for(LodTableColumn col : removedColumns) {
			record.removeColumn(col);
		}
		
		Map<Integer, Integer> indexTranslationInverse = propertyIndicesInverse.get(record.getTableId());
		
		if(indexTranslationInverse!=null) {
	    	for(TableColumn c : record.getSchema().getRecords()) {
	    		
	    		if(!indexTranslationInverse.containsKey(c.getColumnIndex())) {
	    			System.out.println(String.format("No property indices found for table [%d] %s column %d (%d total for table) {%s}", record.getTableId(), record.getPath(), c.getColumnIndex(), indexTranslationInverse.size(), StringUtils.join(indexTranslationInverse.keySet(), ",")));
	    		} else {
		    		int globalId = indexTranslationInverse.get(c.getColumnIndex());
		    		//globalId will be set as column index
		    		MatchableLodColumn mc = new MatchableLodColumn(record.getTableId(), c, globalId);
		
		    		resultCollector.next(mc);
	    		}
			}
		} else {
			System.out.println(String.format("No property indices found for table [%d] %s", record.getTableId(), record.getPath()));
		}
	}

}
