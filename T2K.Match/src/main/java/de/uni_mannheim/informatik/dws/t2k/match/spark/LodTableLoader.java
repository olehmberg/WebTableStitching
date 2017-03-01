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

import java.io.File;
import java.io.StringReader;
import java.util.Map;

import de.uni_mannheim.informatik.dws.t2k.webtables.Table;
import de.uni_mannheim.informatik.dws.t2k.webtables.parsers.LodCsvTableParser;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.processing.DatasetIterator;
import de.uni_mannheim.informatik.wdi.processing.RecordMapper;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class LodTableLoader implements RecordMapper<Pair<String, String>, Table> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private Map<String, Integer> tableIds;
	
	public LodTableLoader(Map<String, Integer> tableIds) {
		this.tableIds = tableIds;
	}
	
	@Override
	public void mapRecord(Pair<String, String> record, DatasetIterator<Table> resultCollector) {
		LodCsvTableParser lodParser = new LodCsvTableParser();
		lodParser.setUseStringCache(false);
    	
    	String fileName = record.getFirst();
    	String content = record.getSecond();
    	
    	StringReader reader = new StringReader(content);
    	
		try {
			Table tDBp = lodParser.parseTable(reader, fileName);
			
			if(tDBp!=null && tDBp.getSchema().getSize()>1 && "rdf-schema#label".equals(tDBp.getSchema().get(1).getHeader())) {
				String path = new File(tDBp.getPath()).getName();
				tDBp.setTableId(tableIds.get(path));
				
				resultCollector.next(tDBp);
			}
			
			
		} catch(Exception e) {
			e.printStackTrace();
		}
	}

}
