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
import java.io.IOException;

import com.beust.jcommander.Parameter;

import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.tnt.match.stitching.FunctionalDependencyDiscovery;
import de.uni_mannheim.informatik.dws.winter.utils.Executable;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.writers.JsonTableWriter;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class DiscoverFunctionalDependencies extends Executable {

	@Parameter(names = "-web", required=true)
	private String webLocation;
	
	@Parameter(names = "-csv", required=true)
	private String csvLocation;
	
	@Parameter(names = "-json", required=true)
	private String jsonLocation;
	
	@Parameter(names = "-serialise")
	private boolean serialise;
	
	public static void main(String[] args) throws IOException {
		DiscoverFunctionalDependencies app = new DiscoverFunctionalDependencies();
		
		if(app.parseCommandLine(DiscoverFunctionalDependencies.class, args)) {
			app.run();
		}
	}
	
	public void run() throws IOException {
		System.err.println("Loading Web Tables");
		WebTables web = WebTables.loadWebTables(new File(webLocation), true, false, false, serialise);
		
		File csvFile = new File(csvLocation);
		csvFile.mkdirs();
		
		System.err.println("Running Functional Dependecy Discovery");
		FunctionalDependencyDiscovery discovery = new FunctionalDependencyDiscovery();
		discovery.run(web.getTables().values(), csvFile);
		
		File jsonFile = new File(jsonLocation);
		jsonFile.mkdirs();
		
		System.err.println("Writing Tables");
		JsonTableWriter jtw = new JsonTableWriter();
		for(Table t : web.getTables().values()) {
			jtw.write(t, new File(jsonFile, t.getPath()));
		}		
		
		System.err.println("Done.");
	}
	
}
