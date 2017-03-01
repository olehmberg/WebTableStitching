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
package de.uni_mannheim.informatik.dws.t2k.match;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;

import com.beust.jcommander.Parameter;

import au.com.bytecode.opencsv.CSVReader;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.t2k.match.data.WebTables;
import de.uni_mannheim.informatik.dws.t2k.utils.cli.Executable;
import de.uni_mannheim.informatik.dws.t2k.webtables.Table;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableColumn;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.ResultSet;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class WriteTablesWithCorrespondences extends Executable {

	@Parameter(names = "-tables", required=true)
	private String tablesLocation;
	
	@Parameter(names = "-schemaCorrespondences")
	private String schemaCorLocation;
	
	public void run() throws NumberFormatException, IOException {
		File tablesFile = new File(tablesLocation);
		
		WebTables web = WebTables.loadWebTables(tablesFile, true, false, false);
		
		loadSchemaCorrespondences(web);
	}
	
	protected void loadSchemaCorrespondences(WebTables web) throws NumberFormatException, IOException {
		CSVReader r = new CSVReader(new FileReader(schemaCorLocation));
		String[] values = null;
		
		ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> result = new ResultSet<>(); 
		
		while((values = r.readNext())!=null) {
			
			String webId = values[0];
			String kbId = values[1];
			Double sim = Double.parseDouble(values[2]);
			
			MatchableTableColumn webMC = web.getSchema().getRecord(webId);
			Table t = web.getTablesById().get(webMC.getTableId());
			TableColumn webCol = t.getSchema().get(webMC.getColumnIndex());
			

			
		}
		
		r.close();
	}
}
