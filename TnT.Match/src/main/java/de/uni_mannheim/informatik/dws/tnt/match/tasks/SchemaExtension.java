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
package de.uni_mannheim.informatik.dws.tnt.match.tasks;

import java.io.File;
import java.io.IOException;

import com.beust.jcommander.Parameter;

import de.uni_mannheim.informatik.dws.tnt.match.TnTTask;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.wdi.matching.MatchingEngine;
import de.uni_mannheim.informatik.wdi.parallel.ParallelDataProcessingEngine;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class SchemaExtension extends TnTTask {


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
	
	@Parameter(names = "-serialise")
	private boolean serialise;
	/**
	 * @param serialise the serialise to set
	 */
	public void setSerialise(boolean serialise) {
		this.serialise = serialise;
	}
	
	
	public static void main(String[] args) throws Exception {
		SchemaExtension se = new SchemaExtension();

		if (se.parseCommandLine(SchemaExtension.class, args)) {

			hello();

			se.initialise();
			se.setDataProcessingEngine(new ParallelDataProcessingEngine());
			se.setMatchingEngine(new MatchingEngine<MatchableTableRow, MatchableTableColumn>());

			se.match();

		}
	}

	private WebTables web;
	private File resultsLocationFile;
	private File evaluationLocationFile;
	
	public void initialise() throws IOException {
		printHeadline("Table Re-Mapping");
		
		// load web tables
		web = WebTables.loadWebTables(new File(webLocation), true, false, false, serialise);
		
		// prepare output directories
    	resultsLocationFile = new File(new File(resultLocation), "remapping");
    	setOutputDirectory(resultsLocationFile);
    	resultsLocationFile.mkdirs();
    	
    	evaluationLocationFile = new File(new File(resultLocation), "evaluation");
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.dws.tnt.match.TnTTask#match()
	 */
	@Override
	public void match() throws Exception {
		
		// iterate over all columns
		
		// if a column is not mapped, test if it can be determined functionally by only mapped columns
		// if yes, output it as schema extension candidate
		// if no, but it can be determined using other columns, output it as complex schema extension candidate
		// else, ignore it
		
	}

}
