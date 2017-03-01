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

import de.uni_mannheim.informatik.dws.t2k.webtables.Table;
import de.uni_mannheim.informatik.dws.t2k.webtables.parsers.CsvTableParser;
import de.uni_mannheim.informatik.dws.t2k.webtables.parsers.JsonTableParser;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.processing.DatasetIterator;
import de.uni_mannheim.informatik.wdi.processing.RecordMapper;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class WebTableLoader implements RecordMapper<Pair<String, String>, Table> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private int maxRows;
	private int maxColumns;
	private int minRows;
	private int minColumns;
	
	public WebTableLoader(int maxRows, int maxColumns, int minRows, int minColumns) {
		this.maxRows = maxRows;
		this.maxColumns = maxColumns;
		this.minRows = minRows;
		this.minColumns = minColumns;
	}

	private boolean convertValues;
	/**
	 * @param convertValues the convertValues to set
	 */
	public void setConvertValues(boolean convertValues) {
		this.convertValues = convertValues;
	}
	
	@Override
	public void mapRecord(Pair<String, String> record, DatasetIterator<Table> resultCollector) {
    	CsvTableParser csvParser = new CsvTableParser();
    	JsonTableParser jsonParser = new JsonTableParser();
    	
    	//TODO add setting for value conversion to csv parser
    	jsonParser.setConvertValues(convertValues);
		
    	String fileName = record.getFirst();
    	String content = record.getSecond();
    	
    	StringReader reader = new StringReader(content);
    	
		try {
			Table web = null;
			
			fileName = new File(fileName).getName();
			
			if(fileName.endsWith("csv")) {
				web = csvParser.parseTable(reader, fileName);
			} else if(fileName.endsWith("json")) {
				web = jsonParser.parseTable(reader, fileName);
			} else {
				System.out.println(String.format("Unknown table format: %s", fileName));
			}
			
			if(web!=null) {
//				discard the table if it has more than maximum rows and columns
				if(web.getRows().size() >= minRows){
					if (web.getColumns().size() >= minColumns){
						if(web.getRows().size() <= maxRows){
							if(web.getColumns().size() <= maxColumns){
								resultCollector.next(web);
							}
						}
					}
				}
			}
			
			
		} catch(Exception e) {
			e.printStackTrace();
		}
	}

}
