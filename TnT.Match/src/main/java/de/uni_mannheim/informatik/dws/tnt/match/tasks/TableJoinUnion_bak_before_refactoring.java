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
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.beust.jcommander.Parameter;

import check_if_useful.MatchingKeyGenerator;
import check_if_useful.WebTablesMatchingGraph;
import de.uni_mannheim.informatik.dws.t2k.utils.StringUtils;
import de.uni_mannheim.informatik.dws.t2k.utils.data.MapUtils;
import de.uni_mannheim.informatik.dws.t2k.utils.data.MapUtils2;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Func;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.t2k.webtables.Table;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.SpecialColumns;
import de.uni_mannheim.informatik.dws.tnt.match.TnTTask;
import de.uni_mannheim.informatik.dws.tnt.match.TableSchemaStatistics;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRowWithKey;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTableMatchingKey;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.tnt.match.evaluation.ClusteringPerformance;
import de.uni_mannheim.informatik.dws.tnt.match.evaluation.N2NGoldStandard;
import de.uni_mannheim.informatik.dws.tnt.match.evaluation.N2NGoldStandardCreator;
import de.uni_mannheim.informatik.dws.tnt.match.matchers.SchemaFreeIdentityResolution;
import de.uni_mannheim.informatik.dws.tnt.match.matchers.TableToTableMatcher;
import de.uni_mannheim.informatik.wdi.matching.MatchingEngine;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.Performance;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.parallel.ParallelDataProcessingEngine;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;
import de.uni_mannheim.informatik.wdi.processing.Function;
import de.uni_mannheim.informatik.wdi.processing.Group;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class TableJoinUnion_bak_before_refactoring extends TnTTask {

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
		TableJoinUnion_bak_before_refactoring tju = new TableJoinUnion_bak_before_refactoring();

		if (tju.parseCommandLine(TableJoinUnion_bak_before_refactoring.class, args)) {

			hello();

			tju.initialise();
			tju.setDataProcessingEngine(new ParallelDataProcessingEngine());
			tju.setMatchingEngine(new MatchingEngine<MatchableTableRow, MatchableTableColumn>());

			tju.match();

		}
	}

	private WebTables web;
	private File resultsLocationFile;
	private File csvResultsLocationFile;
	private File resultsLocationFileFuzzy;
	private File csvResultsLocationFileFuzzy;
	private File resultsLocationFileFuzzySubKey;
	private File csvResultsLocationFileFuzzySubKey;
	private File evaluationLocation;
	
	public void initialise() throws IOException {
		printHeadline("Table Join Union");
		
		// load web tables
		web = WebTables.loadWebTables(new File(webLocation), true, false, false, serialise);
		
		// prepare output directories
    	resultsLocationFile = new File(new File(resultLocation), "join_union");
    	setOutputDirectory(resultsLocationFile);
    	resultsLocationFile.mkdirs();
    	
    	csvResultsLocationFile = new File(new File(resultLocation), "join_union_csv");
    	csvResultsLocationFile.mkdirs();

    	resultsLocationFileFuzzy = new File(new File(resultLocation), "join_union_fuzzy");
    	setOutputDirectory(resultsLocationFileFuzzy);
    	resultsLocationFileFuzzy.mkdirs();
    	
    	csvResultsLocationFileFuzzy = new File(new File(resultLocation), "join_union_fuzzy_csv");
    	csvResultsLocationFileFuzzy.mkdirs();

    	resultsLocationFileFuzzySubKey = new File(new File(resultLocation), "join_union_fuzzy_subkey_json");
    	resultsLocationFileFuzzySubKey.mkdirs();
    	
    	csvResultsLocationFileFuzzySubKey = new File(new File(resultLocation), "join_union_fuzzy_subkey_csv");
    	csvResultsLocationFileFuzzySubKey.mkdirs();
    	
    	evaluationLocation = new File(new File(resultLocation), "evaluation");
    	evaluationLocation.mkdirs();
    	
    	// prepare gold standard
    	N2NGoldStandardCreator gsc = new N2NGoldStandardCreator();
    	gsc.createFromMappedUnionTables(web.getTables().values(), evaluationLocation);
	}

	public void match() throws Exception {

    	/***********************************************
    	 * Schema Statistics
    	 ***********************************************/
    	// maps a table id to its schema
    	final HashMap<Integer, String> tableToSchema = new HashMap<>();
    	// maps a table id to its candidate keys
    	final HashMap<Integer, Collection<Collection<Integer>>> tableToCandidateKeys = new HashMap<>();
    	// maps a schema to a representative table
    	HashMap<String, Table> schemaToTable = new HashMap<>();
    	
    	Map<String, Set<String>> disjointHeaders = new HashMap<>();
    	TableSchemaStatistics stat = new TableSchemaStatistics();
    	for(Table t : web.getTables().values()) {
//    		t.clearProvenance();
    		
    		stat.addTable(t);
    		String schema = stat.generateSchemaString(t);
    		tableToSchema.put(web.getTableIndices().get(t.getPath()), schema);
    		if(!schemaToTable.containsKey(schema)) {
    			schemaToTable.put(schema, t);
    		} 
    		
    		ArrayList<Collection<Integer>> candKeys = new ArrayList<>(t.getSchema().getCandidateKeys().size());
    		for(Collection<TableColumn> key : t.getSchema().getCandidateKeys()) {
    			candKeys.add(Q.project(key, new TableColumn.ColumnIndexProjection()));
    		}
    		tableToCandidateKeys.put(web.getTableIndices().get(t.getPath()), candKeys);
    		
    		for(int i = 0; i < t.getColumns().size(); i++) {
    			TableColumn c1 = t.getSchema().get(i);
    			
    			if(!c1.getHeader().equals("null") && !c1.getHeader().isEmpty()) {
    				Set<String> disjoint = MapUtils.get(disjointHeaders, c1.getHeader(), new HashSet<String>());
    				
	    			for(int j = 0; j < t.getColumns().size(); j++) {
	    				TableColumn c2 = t.getSchema().get(j);
	    				if(i!=j && !c2.getHeader().equals("null") && !c2.getHeader().isEmpty()) {
	    					//TODO don't add headers that appear multiple times in this table!
	    					disjoint.add(c2.getHeader());
	    				}
	    			}
    			}
    		}
    	}
    	stat.print();

    	// create sets of disjoint headers (which cannot have a schema correspondence as they appear in the same table)
    	System.out.println("Disjoint Headers:");
    	for(String header : disjointHeaders.keySet()) {
    		Set<String> disjoint = disjointHeaders.get(header);
    		System.out.println(String.format("\t%s\t%s", header, StringUtils.join(disjoint, ",")));
    	}
    	

    	WebTablesMatchingGraph wtmg = new WebTablesMatchingGraph(web.getTables().values());
    	wtmg.setDisjointHeaders(disjointHeaders);
    	
    	/***********************************************
    	 * Schema-Free Identity Resolution
    	 ***********************************************/
    	//TODO convert Schema-Free Identity Resolution into a blocker
    	SchemaFreeIdentityResolution sfir = new SchemaFreeIdentityResolution();
    	ResultSet<Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>> blockedPairs = sfir.run(web, proc, tableToCandidateKeys);
    	
    	System.out.println(String.format("\t%d table-to-table links", blockedPairs.size()));
    	printValueBasedLinks(blockedPairs, proc, tableToSchema, tableToCandidateKeys);
    	
    	blockedPairs = proc.sort(blockedPairs, new Function<String, Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String execute(
					Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> input) {
				return input.getKey();
			}
    		
    	});
    	
    	// blockedPairs contains all the table-to-table links
    	for(Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> group : blockedPairs.get()) {
    		// determine the tables from the grouping key
    		String[] ids = group.getKey().split("/");
    		int id1 = Integer.parseInt(ids[0]);
    		int id2 = Integer.parseInt(ids[1]);
    		Table t1 = web.getTables().get(id1);
    		Table t2 = web.getTables().get(id2);   	

    		// only add the correspondences if they are new (they will be encountered in both directions: a->b and b->a)
    		if(wtmg.getInstanceCorrespondences(t1, t2)==null) {
    			
    			ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> correspondences = new ResultSet<>();
    			
	    		// iterate over all instance correspondences
	    		for(Pair<MatchableTableRowWithKey, MatchableTableRowWithKey> link : group.getRecords().get()) {
	    			// add the links between the instances
	    			correspondences.add(new Correspondence<MatchableTableRow, MatchableTableColumn>(link.getFirst().getRow(), link.getSecond().getRow(), 1.0, null));
	    		}
	    		//TODO up to this point, all can go to the blocker
	    		//TODO the following should be a matcher using the blocker's result
	    		// -- the schema matching part can also work on all schemas at the same time, if the special columns are removed beforehand
	    		// --maybe matching key generation and identity resolution can be performed in a later step ... initial matching keys are not that reliable anyway ...
	    		TableToTableMatcher t2t = new TableToTableMatcher();
	    		t2t.match(t1, t2, correspondences, web, matchingEngine, proc);
	    		
	    		wtmg.addInstanceCorrespondence(t1, t2, correspondences);
	    		
	    		//if(t2t.getInstanceMapping()!=null && t2t.getInstanceMapping().size()>0) {
	    		if(t2t.getMatchingKeys()!=null) {
	    			wtmg.addSchemaCorrespondences(t1, t2, t2t.getSchemaMapping());
//	    			wtmg.addInstanceCorrespondence(t1, t2, t2t.getInstanceMapping());
//	    			wtmg.addMatchingKeys(t1, t2, t2t.getMatchingKeys());
	    		}
    		}
    	}
    	
    	/** print matching graph **/
    	System.out.println(wtmg.formatMatchingGraph());
    	
		// after matching the schemas of all tables, propagate information about impossible matches in the graph
		// - if a schema correspondence cannot be created between two tables, there cannot be a path via other tables that represents the same correspondence
    	
    	// tables A, B, C
    	// keys: {nr},{name}
    	// join: A.nr <-> B.nr
    	// join B.nr <-> C.nr 
    	// join A.name <-> C.name
    	// -> A.nr <-> C.nr <- if this causes a conflict, there is an error in the graph of join keys!
    	// -- a conflict exists if there is an edge by instance correspondences but no edge by a schema correspondence
    	// -- apply transitivity rule to all schema correspondences in the graph and check for conflicts (schema correspondences already contain the conflicts, as similarity is proportional to the relative number of conflicts)
    	// -- -> all transitive edges already exist, only need to check similarity value??? 
    	
    	// for each cluster in the graph of join keys
    	//  for each table in the cluster
    	//    materialise transitivity for schema correspondences:
    	//		for each pair of an incoming arc and an outgoing arc which share the same column of the current table
    	//		  create a (virtual) schema correspondence in the two connected tables
    	//		  check these correspondences for conflicts (they are already in the set of schema correspondences with a certain similarity, otherwise no record voted for this combination)
    	//	      if there is a conflict (schema correspondence score != 1.0), mark the correspondence as conflicting
    	//        mark the join keys that include the columns from the correspondence as conflicting
    	//		  propose the schema correspondence with maximum similarity as additional key
    	//		  propagate the conflict state along all edges with the conflicting join keys
    	
    	// keys must be extended! find non-conflicting, larger keys (don't try to merge the conflicting keys, as they conflict)
    	// -- we can add any attribute to the conflicting candidate keys to make them larger
    	// -- this is valid as keys are discovered from data and we expect them to be under-estimated
    	// -- if we use the schema correspondence with the highest similarity value, we know that the new key will be the one with the minimum number of additional conflicts
    	// -- if no additional schema correspondences exist, remove the connection completely
    	// check existing instance correspondences for subsets that are consistent w.r.t. a larger key
    	// basically, use result from duplicate-based schema matching and the attributes from the correspondence with the highest similarity < 1 to the key
    	// check if the graph is conflict free with the correspondences created from the new keys (use subset of instance correspondences to check for conflicts)
    	
    	/***********************************************
    	 * Add correspondences via transitivity
    	 ***********************************************/
    	
//    	int ttlCor1 = wtmg.countSchemaCorrespondences();
    	System.out.println("***** Materialising Schema Correspondence Transitivity *****");
//    	int transitivityCount = 
		wtmg.materialiseSchemaCorrespondenceTransitivity();
    	//TODO also propagate candidate keys here?
//    	int ttlCor2 = wtmg.countSchemaCorrespondences();
    	
    	//TODO refactor the following block as a method in the web tables matching graph
    	// now check all instance correspondences
    	List<Table> tablesWithCors = wtmg.getTablesWithSchemaCorrespondences();
//    	for(Table t1 : tablesWithCors) {
//    		List<Table> connectedTables = wtmg.getTablesConnectedViaSchemaCorrespondences(t1);
//    		
//    		for(Table t2 : connectedTables) {
//    			
////    			if(wtmg.getInstanceCorrespondences(t1, t2)!=null) {
//    			
//    			// TODO we re-create all matching keys and instance correspondences here anyway, so no need to calculate them earlier
//    			
//	    			TableToTableMatcher t2t = new TableToTableMatcher();
//	    			t2t.runIdentityResolution(t1, t2, wtmg.getInstanceCorrespondences(t1, t2), wtmg.getSchemaCorrespondences(t1, t2), web, matchingEngine, proc);
//	    			
//	    			wtmg.removeInstanceCorrespondences(t1, t2);
//	    			wtmg.removeMatchingKeys(t1, t2);
//	    			
//	    			if(t2t.getMatchingKeys()!=null) {
//	    				wtmg.addInstanceCorrespondence(t1, t2, t2t.getInstanceMapping());
//	    				wtmg.addMatchingKeys(t1, t2, t2t.getMatchingKeys());
//	    			} else {
//	    				System.out.println("No Matching Key");
//	    			}
////    			}
//    			
//    		}
//    	}
    	
    	// merge tables and write
    	int nextTableId = Q.max(web.getTables().keySet()) + 1;
    	WebTablesMatchingGraph fused = wtmg.fuseTables(nextTableId);
    	Collection<Table> fusedTables = fused.getAllTables();
    	System.out.println("*** Fused Matching Graph ***");
    	System.out.println(fused.formatMatchingGraph());
    	WebTables.writeTables(fusedTables, resultsLocationFile, csvResultsLocationFile);
    	
    	
    	/***********************************************
    	 * Add correspondences via attribute names
    	 ***********************************************/
		// then, generate synonym sets from all schema correspondences
		// use the synonym sets to resolve the conflicts that were created earlier
    	// - only use synonyms that were created from non context-dependent columns
    	//TODO -- > add context-dependent flag to TableColumn and propagate it in the graph of schema correspondences
		// - if the column headers are synonyms (and at least one row supports the correspondence), create it 
		// - remove all instance correspondences that create conflicts with the new schema correspondences
    	
    	System.out.println("***** Adding Schema Correspondence via Attribute Names *****");
//    	int namesCount = 
		wtmg.addSchemaCorrespondencesViaAttributeNames(web.getTables().values());
//    	int ttlCor3 = wtmg.countSchemaCorrespondences();
    	
    	//TODO find a way to add the missing correspondences (no transitivity, different names, but same attribute)
    	// likely matching if: part of a candidate key, always followed by the same columns, which are also part of a candidate key 
    	// (keys tend to be on the left of the table)
    	// usually, left of keys we only find other keys, or row numbers/ranks
//    	wtmg.addSchemaCorrespondencesViaColumnPositions();
    	
    	// has no effect here ... why?
//    	fused.materialiseMatchingKeyTransitivity();
    	
    	// now check all instance correspondences
//    	tablesWithCors = wtmg.getTablesWithSchemaCorrespondences();
//    	for(Table t1 : tablesWithCors) {
//    		List<Table> connectedTables = wtmg.getTablesConnectedViaSchemaCorrespondences(t1);
//    		
//    		for(Table t2 : connectedTables) {
//    			
//    			// if no instance correspondences exist, we cannot remove any incorrect ones ...
//    			// otherwise, we can also use the initial correspondences here
//    			if(wtmg.getInstanceCorrespondences(t1, t2)!=null) {
//	    			TableToTableMatcher t2t = new TableToTableMatcher();
//	    			t2t.runIdentityResolution(t1, t2, wtmg.getInstanceCorrespondences(t1, t2), wtmg.getSchemaCorrespondences(t1, t2), web, matchingEngine, proc);
//	    			
//	    			wtmg.removeInstanceCorrespondences(t1, t2);
//	    			wtmg.removeMatchingKeys(t1, t2);
//	    			
//	    			if(t2t.getMatchingKeys()!=null) {
//	    				wtmg.addInstanceCorrespondence(t1, t2, t2t.getInstanceMapping());
//	    				wtmg.addMatchingKeys(t1, t2, t2t.getMatchingKeys());
//	    			} else {
//	    				System.out.println("No Matching Key");
//	    			}
//    			}
//    			
//    		}
//    	}
    	
//    	/** print modified matching graph **/
//    	System.out.println(wtmg.formatMatchingGraph());
//    	
//    	System.out.println(String.format("Schema Correspondence Count: %d", ttlCor1));
//    	System.out.println(String.format("Added %d correspondences via transitivity", transitivityCount));
//    	System.out.println(String.format("Schema Correspondence Count: %d", ttlCor2));
//    	System.out.println(String.format("Added %d correspondences via attribute names", namesCount));
//    	System.out.println(String.format("Schema Correspondence Count: %d", ttlCor3));
    	
//    	System.out.println("***** Frequent Item Sets for Column Indices *****");
//    	wtmg.addSchemaCorrespondencesViaColumnIndices();
    	
    	/***********************************************
    	 * Materialise Matching Key Transitivity
    	 ***********************************************/
    	
    	System.out.println("***** Materialising Matching Key Transitivity *****");
    	//wtmg.materialiseMatchingKeyTransitivity();
    	wtmg.propagateMatchingKeys();
		
		// if A is a key of t1 and all columns of A are mapped to t2, but they are not a key nor do they overlap with a key of t2, 
		// then they are not unique in t2 and we cannot use them to merge the schemas without making errors.
		
		// as A is not unique in t2, we must be able to find a value of t2's keys in t1, otherwise there is no possibility for matching keys
		
		// Further, we don't know how to extend them (otherwise we would have done so above).
		// So, to be safe, we remove the keys such that no instance correspondences between the tables can be created
		//TODO remove invalid keys
		// that would also remove complete and correct keys in a 1:n relationship ...
		
    	
    	//TODO t1 with key {a} and t2 with key {b,c} won't change by propagating matching keys, as their existing keys do not overlap
    	// propagate {b,c} if it is completely mapped? - only needed if {b,c} is *not* a key in t1. But then we have to extend it to a key...
    	// ---> if {b,c} is not a key in t1, it is an underestimated key in t2, so we have to find a better key. But if we extend it with an unmapped column, we cannot check the new key
    	// in this case, we would need to find the value of {a} in t2 to get a unique key and be able to map {a}U{b,c}
    	// -- but, as we don't have this schema correspondence from duplicate-based schema matching, we know that the records in tables t1 and t2 do not overlap
    	// -- can we be sure that this combination was checked? the keys might not have been compared as they don't match in size ...
    	// so, again, we cannot make use of the keys to merge the schemas

    	// easier: propagate *not* keys in the graph: if any table has key {a,b}, then {a} alone should not be a key used for matching (keep the correspondences, but don't create matching keys)
    	// should we remove the key {a} completely?

    	// TODO if matching keys are to small, and instance correspondences cause a conflict in mapped columns, these columns must become part of the key!
//    	  Schema Correspondences:
//    		   (1,00000000) [2] side e titles <-> [2] side 2 titles
//    		   (1,00000000) [4] length <-> [4] length
//    		   (1,00000000) [3] artist <-> [3] artist
//    		  Matching Keys:
//    		   {side e titles} <-> {side 2 titles}
//    		   1438042989018.40_201 | 1                    | 2. i shot the sherif | bob marley & the wai | 05:25               
//    		   1438042989301.17_201 | 1                    | 2. i shot the sherif | bob marley           | 05:07               
    	
    	// now check all instance correspondences
    	tablesWithCors = wtmg.getTablesWithSchemaCorrespondences();
    	for(Table t1 : tablesWithCors) {
    		List<Table> connectedTables = wtmg.getTablesConnectedViaSchemaCorrespondences(t1);
    		
    		for(Table t2 : connectedTables) {
    			
    			if(wtmg.getInstanceCorrespondences(t1, t2)!=null) {
	    			TableToTableMatcher t2t = new TableToTableMatcher();
	    			t2t.runIdentityResolution(t1, t2, wtmg.getInstanceCorrespondences(t1, t2), wtmg.getSchemaCorrespondences(t1, t2), web, matchingEngine, proc);
	    			
	    			wtmg.removeInstanceCorrespondences(t1, t2);
	    			wtmg.removeMatchingKeys(t1, t2);
	    			
	    			if(t2t.getMatchingKeys()!=null) {
	    				wtmg.addInstanceCorrespondence(t1, t2, t2t.getInstanceMapping());
	    				wtmg.addMatchingKeys(t1, t2, t2t.getMatchingKeys());
	    			} else {
	    				System.out.println("No Matching Key");
	    			}
    			}
    			
    		}
    	}
    	
    	// TODO from the beginning to this point, it should be sufficient to run identity resolution only once
    	// -- instance correspondences do affect the schema matching, wrong instance correspondences prevent possibly correct schema correspondences
    	// -- -- with a wrong instance correspondence, the number of votes needed for 100% similarity is higher than the number of votes that can be cast by the correct instance correspondences
    	// -- instance correspondences do not affect the schema inference
    	// -- instance correspondences do not affect the matching key generation
    	// TODO after running identity resolution, we should run duplicate-based schema matching again
    	// -- after duplicate-based schema matching, identity resolution needs not be run again, as only perfectly matching schema correspondences are created
    	// -- but, applying transitivity and synonyms could lead to new schema correspondences that invalidate existing instance correspondences
    	// -- however, we never have to look for new instance correspondences?
    	// -- -- partially true, as all keys are underestimated and we have all possible correspondences for these keys
    	// -- -- but, we might have missed some correspondences in schema-free identity resolution as we only compared keys of equal size
    	// -- -- so we have to re-check table combinations with updated key sizes ...
    	// TODO one option would be to run until we can propagate the key information and then start over with the new keys, looping until no keys change anymore (but keep schema correspondences)
    	// -- run 
    	// 														--- algorithm (1) ---
    	//					A method to perform instance and schema matching on any number of tables with no prior knowledge about the data
    	// -- -- Schema-free Identity Resolution (sfir)							blocking, gives initial instance correspondences
    	// -- -- Duplicate-based Schema Matching (dbsm)							initial schema matching
    	// -- -- Join-based Identity Resolution (jbir)							identity resolution for the keys, removes obvious errors
    	// 														--- algorithm (1) ---
    	// -- -- Materialise Schema Correspondence Transitivity (msct)			adds more schema correspondences
    	// -- -- Schema Matching via Attribute Synonyms (smas)					adds more schema correspondences
    	// -- -- Propagate Matching Keys (pmk)									updates the keys
    	// TODO if an instance correspondence matches on the key values, but not on other values, it will survive identity resolution
    	// -- the data conflict then shows that the keys in the tables & the matching key are underestimated -- OR -- that there is a 1:n relationship
    	// -- in case of a 1:n relationship, we should find multiple instance correspondences for the n table and one for the 1 table (can only happen when using partial matching keys)
    	// -- in case of an underestimated key, we should find only one instance correspondence per table (otherwise, the key would not be unique in the tables)
    	
    	/** print modified matching graph **/
    	System.out.println(wtmg.formatMatchingGraph());
    	wtmg.writeMatchingKeyGraph(new File(resultsLocationFileFuzzy.getParentFile(), "join_union_fuzzy.net"));
    	
    	nextTableId = Q.max(web.getTables().keySet()) + 1;
    	fused = wtmg.fuseTables(nextTableId);
    	fusedTables = fused.getAllTables();
    	System.out.println("*** Fused Matching Graph ***");
    	System.out.println(fused.formatMatchingGraph());
//    	fusedTables = wtmg.fuseTables(nextTableId);
    	WebTables.writeTables(fusedTables, resultsLocationFileFuzzy, csvResultsLocationFileFuzzy);
    	
    	
    	/***********************************************
    	 * Merge the fused graph by partial-key matching
    	 ***********************************************/
    	System.out.println("### Partial-Key Matching ###");
    	
    	// now check all instance correspondences
    	tablesWithCors = fused.getTablesWithSchemaCorrespondences();
    	for(Table t1 : tablesWithCors) {
    		List<Table> connectedTables = fused.getTablesConnectedViaSchemaCorrespondences(t1);
    		
    		for(Table t2 : connectedTables) {
    			

				// avoid generating unnecessary keys
				/*
				  Matching Keys:
				   {artist,price} <-> {title,artist,price}   <--- both are full candidate keys
				   the following keys are no necessary:
   				   {price} <-> {title,artist,genre,price}
				   {price} <-> {title,genre,price}
				   {price} <-> {title,date added,artist,price}
				   {price} <-> {title,date added,price}
				*/
    			
    			//TODO iteratively propagating the candidate keys means we have to continue until no changes occur anymore ... can we solve it cluster-based?
    			
				MatchingKeyGenerator mkg = new MatchingKeyGenerator();
				
				Set<WebTableMatchingKey> keys = fused.getMatchingKeys(t1, t2); 
//				= mkg.generateAllJoinKeysFromCorrespondences(t1, t2, fused.getSchemaCorrespondences(t1, t2), 1.0);
				
				if(keys!=null) {
					// we found at least one full key match, we don't need partial key matches
//					fused.removeMatchingKeys(t1, t2);
//					fused.addMatchingKeys(t1, t2, keys);
					
				} else {
					// we didn't find full key matches, look for partial key matches
					Set<WebTableMatchingKey> subkeys 
					= mkg.generateMaximalSubJoinKeysFromCorrespondences(t1, t2, fused.getSchemaCorrespondences(t1, t2), 1.0);
					
					if(subkeys!=null) {
	
						System.out.println(String.format("Sub Keys for {%s}<->{%s}:", t1.getPath(), t2.getPath()));
						for(WebTableMatchingKey p : subkeys) {
							System.out.println(String.format("{%s}<->{%s}", StringUtils.join(Q.project(p.getFirst(), new TableColumn.ColumnHeaderProjection()), ","), StringUtils.join(Q.project(p.getSecond(), new TableColumn.ColumnHeaderProjection()), ",")));
						}
						
						fused.addMatchingKeys(t1, t2, subkeys);
					
						//TODO join-based identity resolution using the sub keys
						// run schema-free identity resolution cannot create any new links, as we added constraints (so now we get less links than before)
						// - schema-free identity resolution will generate new links as the tables have never been looked at before if their keys didn't match completely
						// instance correspondences should reveal arity of relationship: 1:1, 1:n, n:m
						// partial key matching can be problematic: a schema about songs and a schema about albums matched via artist as partial key of both tables ...
						
					}
    			}
    		}
    	}
    	
    	// works here, but it's too late as the rows with incorrect correspondences are already merged
//    	fused.materialiseMatchingKeyTransitivity();
    	
    	nextTableId = Q.max(web.getTables().keySet()) + 1;
    	System.out.println("*** Matching Graph with partial key matching ***");
    	System.out.println(fused.formatMatchingGraph());
    	fused = fused.fuseTables(nextTableId);
    	fusedTables = fused.getAllTables();
    	System.out.println("*** Fused Matching Graph ***");
    	System.out.println(fused.formatMatchingGraph());
    	//TODO create a table info summary (keys, dependencies, etc.) and write it as final result (text, human readable)
    	WebTables.writeTables(fusedTables, resultsLocationFileFuzzySubKey, csvResultsLocationFileFuzzySubKey);
    	
    	
    	/***********************************************
    	 * Write Correspondences & Evaluate
    	 ***********************************************/
    	// write all provenance data for gold standard creation and evaluation
    	N2NGoldStandard n2n = new N2NGoldStandard();
    	HashSet<String> allTables = new HashSet<>();
    	for(Table t : fusedTables) {
    		
    		for(TableColumn c : t.getColumns()) {
    			
    			Set<String> clu = new HashSet<>();
    			
    			if(!SpecialColumns.isSpecialColumn(c)) {
    				
    				clu.addAll(c.getProvenance());
    			
    				for(String prov : c.getProvenance()) {
    					allTables.add(prov.split(";")[0]);
    				}
//    				allTables.addAll(c.getProvenance());
    			}
    			
    			n2n.getCorrespondenceClusters().put(clu, String.format("{%s}[%d]%s", t.getPath(), c.getColumnIndex(), c.getHeader()));
    			
    		}
    		
    	}
    	
    	n2n.writeToTSV(new File(evaluationLocation, "correspondences_join_union.tsv"));
    	
    	N2NGoldStandard gs = new N2NGoldStandard();
    	File gsFile = new File(evaluationLocation, "goldstandard.tsv");
    	if(gsFile.exists()) {
	    	gs.loadFromTSV(gsFile);

	    	ClusteringPerformance perf = gs.evaluateCorrespondenceClusters(n2n.getCorrespondenceClusters(), false);
	    	
	    	System.out.println("*** Evaluation ***");
	    	System.out.println(String.format("\t%d tables in all correspondences", allTables.size()));
	    	for(String key : perf.getPerformanceByCluster().keySet()) {
	    		Performance p = perf.getPerformanceByCluster().get(key);
	    		System.out.println(String.format("%s:\tprec: %.4f\trec:%.4f\tf1:%.4f\t\t%d/%d correspondences", org.apache.commons.lang3.StringUtils.rightPad(key, 30), p.getPrecision(), p.getRecall(), p.getF1(), p.getNumberOfPredicted(), p.getNumberOfCorrectTotal()));
	    	}
    	}
    }

	/***
	 * 
	 * @param blockedPairs
	 * @param proc
	 * @param tableToSchema
	 * @param tableToCandidateKeys
	 * @return a map table->table->key->mapped rows
	 */
	private Map<Table, Map<Table, Map<String, Set<Integer>>>> printValueBasedLinks(
			ResultSet<Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>> blockedPairs, 
			DataProcessingEngine proc,
			final HashMap<Integer, String> tableToSchema,
			final HashMap<Integer, Collection<Collection<Integer>>> tableToCandidateKeys) {
		
		// sort all the groups by (from table),(to table)
    	System.out.println("sorting");
//    	blockedPairs = proc.sort(blockedPairs, new Comparator<Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>>() {
//
//			@Override
//			public int compare(
//					Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> o1,
//					Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> o2) {
//				
//				return o1.getKey().compareTo(o2.getKey());
//			}
//		});
    	
    	blockedPairs = proc.sort(blockedPairs, new Function<String, Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>>() {
			private static final long serialVersionUID = 1L;

			@Override
			public String execute(
					Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> input) {
				return input.getKey();
			}
    		
    	});
    	
    	Map<String, Map<String, Set<String>>> tableLinksViaKey = new HashMap<>();
    	Map<Table, Map<Table, Map<String, Set<Integer>>>> allLinks = new HashMap<>();
    	
    	// print all the links
    	for(Group<String, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> group : blockedPairs.get()) {
    		
    		// determine tables from group key
    		String[] ids = group.getKey().split("/");
    		int id1 = Integer.parseInt(ids[0]);
    		int id2 = Integer.parseInt(ids[1]);
    		final Table t1 = web.getTables().get(id1);
    		final Table t2 = web.getTables().get(id2);
    		
    		// count key occurrences
    		HashMap<String, Set<Integer>> keyCounts1 = new HashMap<>();
    		HashMap<String, Set<Integer>> keyCounts2 = new HashMap<>();
    		
    		for(Collection<Integer> key : tableToCandidateKeys.get(id1)) {
    			String s = StringUtils.join(Q.project(key, new Func<String, Integer>() {

					@Override
					public String invoke(Integer in) {
						return String.format("[%d]%s", in,  t1.getSchema().get(in).getHeader());
					}
				}), ",");
    			keyCounts1.put(s, new HashSet<Integer>());
    		}
    		for(Collection<Integer> key : tableToCandidateKeys.get(id2)) {
    			String s = StringUtils.join(Q.project(key, new Func<String, Integer>() {

					@Override
					public String invoke(Integer in) {
						return String.format("[%d]%s", in, t2.getSchema().get(in).getHeader());
					}
				}), ",");
    			keyCounts2.put(s, new HashSet<Integer>());
    		}
    		
    		System.out.println(String.format("%s <-> %s", tableToSchema.get(id1), tableToSchema.get(id2)));
    		
    		ResultSet<Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>> sorted = proc.sort(group.getRecords(), new Function<Integer, Pair<MatchableTableRowWithKey, MatchableTableRowWithKey>>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Integer execute(
						Pair<MatchableTableRowWithKey, MatchableTableRowWithKey> input) {
					return input.getFirst().getRow().getRowNumber();
				}
    			
    		});
    		
    		for(final Pair<MatchableTableRowWithKey, MatchableTableRowWithKey> link : sorted.get()) {
    			
    			String key1 = StringUtils.join(Q.project(link.getFirst().getKey(), new Func<String, Integer>() {

					@Override
					public String invoke(Integer in) {
						return String.format("[%d]%s", in,  t1.getSchema().get(in).getHeader());
					}
				}), ",");
    			String key2 = StringUtils.join(Q.project(link.getSecond().getKey(), new Func<String, Integer>() {

					@Override
					public String invoke(Integer in) {
						return String.format("[%d]%s", in, t2.getSchema().get(in).getHeader());
					}
				}), ",");
    			
    			keyCounts1.get(key1).add(link.getFirst().getRow().getRowNumber());
    			keyCounts2.get(key2).add(link.getSecond().getRow().getRowNumber());
    			
    			System.out.println(String.format("{%s}->{%s} : [%s]", key1, key2, StringUtils.join(Q.project(link.getFirst().getKey(), new Func<String, Integer>() {

					@Override
					public String invoke(Integer in) {
						return (String)link.getFirst().getRow().get(in);
					}
				}), ",")));
    			System.out.println(String.format("\t%s", link.getFirst().getRow().format(20)));
    			System.out.println(String.format("\t%s", link.getSecond().getRow().format(20)));
    			System.out.println();
    		}
    		
    		Map<String, Set<String>> keys = MapUtils.get(tableLinksViaKey, t1.getPath(), new HashMap<String, Set<String>>());
    		
    		// summarize key counts
    		System.out.println(t1.getPath());
    		for(String s : keyCounts1.keySet()) {
    			System.out.println(String.format("%s : %d", s, keyCounts1.get(s).size()));
    			
    			if(keyCounts1.get(s).size()>0) {
	    			Set<String> linkedTables = MapUtils.get(keys, s, new HashSet<String>());
	    			linkedTables.add(t2.getPath());
	    			
	    			MapUtils2.put(allLinks, t1, t2, keyCounts1);
    			}
    		}
    		System.out.println(t2.getPath());
    		for(String s : keyCounts2.keySet()) {
    			System.out.println(String.format("%s : %d", s, keyCounts2.get(s).size()));
    			
    			MapUtils2.put(allLinks, t2, t1, keyCounts2);
    		}
    	}
    	
    	// summarize table linkage via keys
    	System.out.println("Table linkage via keys");
    	for(String t1 : tableLinksViaKey.keySet()) {
    		System.out.println("\t" + t1);
    		Map<String, Set<String>> keys = tableLinksViaKey.get(t1);
    		
    		// count number of keys linking to each table
    		Map<String, Set<String>> tables = new HashMap<>();
    		
    		for(String key : keys.keySet()) {
    			Set<String> linkedTables = keys.get(key);
    			System.out.println(String.format("\t\t%d\t%s\t%s", linkedTables.size(), key, StringUtils.join(linkedTables, ",")));
    			
    			for(String table : linkedTables){
    				Set<String> linkingKeys = MapUtils.get(tables, table, new HashSet<String>());
    				linkingKeys.add(key);
    			}
    		}
    		
    		for(String table : tables.keySet()) {
    			Set<String> linkingKeys = tables.get(table);
    			if(linkingKeys.size()>1) {
    				System.out.println(String.format("\t\t%d\t%s\t%s", linkingKeys.size(), table, StringUtils.join(linkingKeys, "||")));
    			}
    		}
    	}
    	
    	return allLinks;
	}
	
}
