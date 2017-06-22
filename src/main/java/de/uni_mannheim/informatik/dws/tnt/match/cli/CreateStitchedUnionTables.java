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
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import com.beust.jcommander.Parameter;

import de.uni_mannheim.informatik.dws.tnt.match.DisjointHeaders;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.tnt.match.matchers.CandidateKeyBasedMatcher;
import de.uni_mannheim.informatik.dws.tnt.match.matchers.DeterminantBasedMatcher;
import de.uni_mannheim.informatik.dws.tnt.match.matchers.EntityLabelBasedMatcher;
import de.uni_mannheim.informatik.dws.tnt.match.matchers.LabelBasedMatcher;
import de.uni_mannheim.informatik.dws.tnt.match.matchers.TableToTableMatcher;
import de.uni_mannheim.informatik.dws.tnt.match.matchers.ValueBasedMatcher;
import de.uni_mannheim.informatik.dws.tnt.match.stitching.StitchedUnionTables;
import de.uni_mannheim.informatik.dws.winter.matching.MatchingEngine;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.utils.Executable;
import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.writers.JsonTableWriter;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class CreateStitchedUnionTables extends Executable {

	public static enum MatcherType {
		Trivial,
		NonTrivialPartial,
		NonTrivialFull,
		CandidateKey,
		Label,
		Entity
	}
	
	@Parameter(names = "-matcher")
	private MatcherType matcher = MatcherType.NonTrivialFull;
	
	@Parameter(names = "-web", required=true)
	private String webLocation;
	
	@Parameter(names = "-results", required=true)
	private String resultLocation;
	
	@Parameter(names = "-serialise")
	private boolean serialise;
	
	public static void main(String[] args) throws Exception {
		CreateStitchedUnionTables app = new CreateStitchedUnionTables();
		
		if(app.parseCommandLine(CreateStitchedUnionTables.class, args)) {
			app.run();
		}
	}
	
	public void run() throws Exception {
		System.err.println("Loading Web Tables");
		WebTables web = WebTables.loadWebTables(new File(webLocation), true, false, false, serialise);
		web.removeHorizontallyStackedTables();
		
		System.err.println("Matching Union Tables");
		Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences = runTableMatching(web);
		
		System.err.println("Creating Stitched Union Tables");
		StitchedUnionTables stitchedUnion = new StitchedUnionTables();
		Collection<Table> reconstructed = stitchedUnion.create(web.getTables(), web.getRecords(), web.getSchema(), web.getCandidateKeys(), schemaCorrespondences);
		
		File outFile = new File(resultLocation);
		outFile.mkdirs();
		System.err.println("Writing Stitched Union Tables");
		JsonTableWriter w = new JsonTableWriter();
		for(Table t : reconstructed) {
			w.write(t, new File(outFile, t.getPath()));
		}
		
		System.err.println("Done.");
	}
	
	private Processable<Correspondence<MatchableTableColumn, Matchable>> runTableMatching(WebTables web) throws Exception {
		TableToTableMatcher matcher = null;
		
    	switch(this.matcher) {
		case CandidateKey:
			matcher = new CandidateKeyBasedMatcher();
			break;
		case Label:
			matcher = new LabelBasedMatcher();
			break;
		case NonTrivialFull:
			matcher = new DeterminantBasedMatcher();
			break;
		case Trivial:
			matcher = new ValueBasedMatcher();
			break;
		case Entity:
			matcher = new EntityLabelBasedMatcher();
			break;
		default:
			break;
    		
    	}
    	
    	DisjointHeaders dh = DisjointHeaders.fromTables(web.getTables().values());
    	Map<String, Set<String>> disjointHeaders = dh.getAllDisjointHeaders();
    	
    	matcher.setWebTables(web);
    	matcher.setMatchingEngine(new MatchingEngine<>());
    	matcher.setDisjointHeaders(disjointHeaders);
		
    	matcher.initialise();
    	matcher.match();
    	
    	Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences = matcher.getSchemaCorrespondences();
    	
    	return schemaCorrespondences;
	}
}
