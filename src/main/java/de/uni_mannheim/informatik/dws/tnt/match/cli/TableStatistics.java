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
package de.uni_mannheim.informatik.dws.tnt.match.cli;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import com.beust.jcommander.Parameter;

import au.com.bytecode.opencsv.CSVWriter;
import de.uni_mannheim.informatik.dws.tnt.match.TnTTask;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class TableStatistics extends TnTTask {

	@Parameter(names = "-web")
	private String webLocation;
	/**
	 * @param webLocation the webLocation to set
	 */
	public void setWebLocation(String webLocation) {
		this.webLocation = webLocation;
	}

	@Parameter(names = "-results")
	private String resultLocation;
	/**
	 * @param resultLocation the resultLocation to set
	 */
	public void setResultLocation(String resultLocation) {
		this.resultLocation = resultLocation;
	}

	public static void main(String[] args) throws Exception {
		TableStatistics tu = new TableStatistics();

		if (tu.parseCommandLine(TableStatistics.class, args)) {

			tu.initialise();

			tu.match();

		}
	}

	private WebTables web;
	private File resultLocationFile;
	
	public void initialise() throws IOException {
		
		// load web tables
		web = WebTables.loadWebTables(new File(webLocation), true, false, false, true);

		// create output directory
		resultLocationFile = new File(resultLocation);
		resultLocationFile.mkdirs();

	}
	
	public void match() throws Exception {
		CSVWriter resultStatisticsWriter = new CSVWriter(new FileWriter(new File(resultLocationFile, "original.csv"), true));
		
		for(Table t : web.getTables().values()) {
			resultStatisticsWriter.writeNext(new String[] {
					new File(webLocation).getName(),
					t.getPath(),
					Integer.toString(t.getRows().size()),
					Integer.toString(t.getColumns().size())
			});
		}
		
		resultStatisticsWriter.close();
		
    	
	}


}
