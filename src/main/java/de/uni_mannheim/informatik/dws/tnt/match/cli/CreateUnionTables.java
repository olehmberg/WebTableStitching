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
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.beust.jcommander.Parameter;

import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.tnt.match.stitching.UnionTables;
import de.uni_mannheim.informatik.dws.winter.utils.Executable;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.writers.JsonTableWriter;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class CreateUnionTables extends Executable {

	@Parameter(names = "-web", required=true)
	private String webLocation;

	@Parameter(names = "-results", required=true)
	private String resultLocation;
	
	@Parameter(names = "-serialise")
	private boolean serialise;
	
	@Parameter(names = "-noContextColumns")
	private boolean noContextColumns = false;
	
	public static void main(String[] args) throws URISyntaxException, IOException {
		CreateUnionTables app = new CreateUnionTables();
		
		if(app.parseCommandLine(CreateUnionTables.class, args)) {
			app.run();
		}
	}
	
	public void run() throws URISyntaxException, IOException {
	
		System.err.println("Loading Web Tables");
		// load web tables
		WebTables web = WebTables.loadWebTables(new File(webLocation), true, false, false, serialise);
		
		UnionTables union = new UnionTables();
		
		Map<String, Integer> contextAttributes = null;
		
		if(!noContextColumns) {
			System.err.println("Creating Context Attributes");
			contextAttributes = union.generateContextAttributes(web.getTables().values(), true, false);
		} else {
			contextAttributes = new HashMap<>();
		}
		
		System.err.println("Creating Union Tables");
		Collection<Table> unionTables = union.create(new ArrayList<>(web.getTables().values()), contextAttributes);
		
		File outFile = new File(resultLocation);
		outFile.mkdirs();
		
		System.err.println("Writing Union Tables");
		JsonTableWriter w = new JsonTableWriter();
		for(Table t : unionTables) {
			w.write(t, new File(outFile, t.getPath()));
		}
		
		System.err.println("Done.");
	}
}
