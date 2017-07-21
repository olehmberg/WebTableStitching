/*
 * Copyright (c) 2017 Data and Web Science Group, University of Mannheim, Germany (http://dws.informatik.uni-mannheim.de/)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and limitations under the License.
 */
package de.uni_mannheim.informatik.dws.tnt.match.cli;

import java.io.File;
import java.io.IOException;

import com.beust.jcommander.Parameter;

import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.tnt.match.evaluation.N2NGoldStandardCreator;
import de.uni_mannheim.informatik.dws.winter.utils.Executable;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class CreateGoldStandard extends Executable {

	@Parameter(names = "-web") 
	private String webLocation;
	
	@Parameter(names = "-eval")
	private String evalLocation;
	
	public static void main(String[] args) throws IOException {
		CreateGoldStandard app = new CreateGoldStandard();
		
		if(app.parseCommandLine(CreateGoldStandard.class, args)) {
			app.run();
		}
	}
	
	public void run() throws IOException {
		WebTables web = WebTables.loadWebTables(new File(webLocation), true, false, false, false);
		
		N2NGoldStandardCreator creator = new N2NGoldStandardCreator();
		
		creator.createFromMappedUnionTables(web.getTables().values(), new File(evalLocation));
	}
	
}
