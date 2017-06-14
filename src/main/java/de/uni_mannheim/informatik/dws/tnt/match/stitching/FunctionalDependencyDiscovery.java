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
package de.uni_mannheim.informatik.dws.tnt.match.stitching;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import de.uni_mannheim.informatik.dws.tnt.match.dependencies.FunctionalDependencyUtils;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.writers.CSVTableWriter;

/**
 * 
 * Deduplicates the Web Tables and discovers functional dependencies & candidate keys
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class FunctionalDependencyDiscovery {

	public void run(Collection<Table> tables, File csvOutput) throws FileNotFoundException {
		
		// redirect std. out (from HyFD)
		PrintStream tmp = new PrintStream(new File("HyFD_UCC.out"));
		final PrintStream out = System.out;
		System.setOut(tmp);
		
		Processable<Table> webTables = new ProcessableCollection<>(tables);

		webTables.iterateDataset(new DataIterator<Table>() {
			private static final long serialVersionUID = 1L;

			@Override
			public void initialise() {
				
			}

			@Override
			public void next(final Table t) {
				StringBuilder logData = new StringBuilder();
				logData.append(t.getPath() + "\n");
				
				try {
					/***********************************************
	    	    	 * De-Duplication
	    	    	 ***********************************************/
	    			// de-duplicate t on all columns (FDs and UCCs would be incorrect with duplicates)
    				t.deduplicate(t.getColumns());

					// write tables as csv to run FD discovery
	    			CSVTableWriter tw = new CSVTableWriter();	    			
	    			File f = tw.write(t, new File(csvOutput, t.getPath()), ',', '"', '\\');
		    		
	    	    	/***********************************************
	    	    	 * Calculate Functional Dependencies
	    	    	 ***********************************************/
					Map<Collection<TableColumn>, Collection<TableColumn>> functionalDependencies = FunctionalDependencyUtils.calculateFunctionalDependencies(t, f);
					t.getSchema().setFunctionalDependencies(functionalDependencies);

		    		
		        	/***********************************************
		        	 * Key Detection
		        	 ***********************************************/
		    		// list all candidate keys
		    		Collection<Set<TableColumn>> candKeysWithContext = FunctionalDependencyUtils.calculateUniqueColumnCombinations(t, f); 
		    		t.getSchema().setCandidateKeys(candKeysWithContext);
				} catch(Exception e) {
					e.printStackTrace();
				}
			}

			@Override
			public void finalise() {

			}
		});
    	
    	System.setOut(out);
	}
	
	
}
