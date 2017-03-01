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
package de.uni_mannheim.informatik.dws.tnt.match;

import java.io.File;
import java.io.IOException;
import com.beust.jcommander.Parameter;

import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.match.tasks.TableDeDuplication;
import de.uni_mannheim.informatik.dws.tnt.match.tasks.TableJoinUnion;
import de.uni_mannheim.informatik.dws.tnt.match.tasks.TableUnion;
import de.uni_mannheim.informatik.wdi.parallel.ParallelDataProcessingEngine;
import de.uni_mannheim.informatik.wdi.parallel.ParallelMatchingEngine;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class TnTMatch extends TnTTask {

	@Parameter(names = "-web")
	private String webLocation;
	
	@Parameter(names = "-results")
	private String resultLocation;

	@Parameter(names = "-measureMemory")
	private boolean measure;

	public static void main(String[] args) throws Exception {
		TnTMatch t2t = new TnTMatch();

		if (t2t.parseCommandLine(TnTMatch.class, args)) {

			hello();

			System.out.println(String.format("Web tables location: %s", new File(t2t.webLocation).getAbsolutePath()));
			System.out.println(String.format("Output location:     %s", new File (t2t.resultLocation).getAbsolutePath()));
			
			t2t.initialise();
			t2t.setDataProcessingEngine(new ParallelDataProcessingEngine());
//			t2t.setMatchingEngine(new MatchingEngine<MatchableTableRow, MatchableTableColumn>());
			t2t.setMatchingEngine(new ParallelMatchingEngine<MatchableTableRow, MatchableTableColumn>());

			t2t.match();

		}
	}

	public void initialise() throws IOException {

	}

	public void match() throws Exception {

    	/***********************************************
    	 * C1 - Union
    	 ***********************************************/
		
		TableUnion tu = new TableUnion();
		tu.setDataProcessingEngine(proc);
		tu.setWebLocation(webLocation);
		tu.setResultLocation(resultLocation);
		tu.initialise();
		tu.match();
		
		TableDeDuplication td = new TableDeDuplication();
		td.setDataProcessingEngine(proc);
		td.setWebLocation(tu.getOutputDirectory().getAbsolutePath());
		td.setResultLocation(resultLocation);
		td.initialise();
		td.match();
		
		/***********************************************
    	 * C2 - Join Union
    	 ***********************************************/
		
		TableJoinUnion tju = new TableJoinUnion();
		tju.setDataProcessingEngine(proc);
		tju.setMatchingEngine(matchingEngine);
		tju.setWebLocation(td.getOutputDirectory().getAbsolutePath());
		tju.setResultLocation(resultLocation);
		tju.initialise();
		tju.match();
		
//		TableDeDuplication td2 = new TableDeDuplication();
//		td2.setDataProcessingEngine(proc);
//		td2.setWebLocation(tju.getOutputDirectory().getAbsolutePath());
//		td2.setResultLocation(resultLocation);
//		td2.initialise();
//		td2.match();
    	
    }

}
