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

import com.beust.jcommander.Parameter;

import de.uni_mannheim.informatik.dws.tnt.match.ContextColumns;
import de.uni_mannheim.informatik.dws.tnt.match.TnTTask;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.parsers.JsonTableParser;
import de.uni_mannheim.informatik.dws.winter.webtables.writers.JsonTableWriter;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class EntityLabelDetection extends TnTTask {

	@Parameter(names = "-web")
	private String webLocation;

	public static void main(String[] args) throws Exception {
		EntityLabelDetection eld = new EntityLabelDetection();
		
		if(eld.parseCommandLine(EntityLabelDetection.class, args)) {
			eld.initialise();
			eld.match();
		}
	}
	
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.dws.tnt.match.TnTTask#initialise()
	 */
	@Override
	public void initialise() throws Exception {
		
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.dws.tnt.match.TnTTask#match()
	 */
	@Override
	public void match() throws Exception {
		
		File webFile = new File(webLocation);
		
		JsonTableParser p = new JsonTableParser();
		p.setConvertValues(false);
		JsonTableWriter w = new JsonTableWriter();
		
		for(File f : webFile.listFiles()) {
			
			System.out.println(String.format("Detecting Entity-Label Column for '%s'", f.getName()));
			
			Table t = p.parseTable(f);
			
			Table noContext = t.project(Q.where(t.getColumns(), new ContextColumns.IsNoContextColumnPredicate()));
			noContext.identifySubjectColumn(0.001, true);
			
			if(noContext.hasSubjectColumn()) {
				t.setSubjectColumnIndex(noContext.getSubjectColumnIndex() + (t.getColumns().size() - noContext.getColumns().size()));
				System.out.println(String.format("Entity-Label Column for '%s' is '%s'", f.getName(), t.getSchema().get(t.getSubjectColumnIndex()).getHeader()));
			} else {
				t.setSubjectColumnIndex(-1);
			}
			
			w.write(t, f);
		}
		
	}

}
