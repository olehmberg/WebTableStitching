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
package de.uni_mannheim.informatik.dws.tnt.match.dependencies;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import de.hpi.metanome.algorithms.hyfd.HyFD;
import de.hpi.metanome.algorithms.hyucc.HyUCC;
import de.metanome.algorithm_integration.AlgorithmExecutionException;
import de.metanome.algorithm_integration.ColumnIdentifier;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;
import de.metanome.algorithm_integration.result_receiver.ColumnNameMismatchException;
import de.metanome.algorithm_integration.result_receiver.CouldNotReceiveResultException;
import de.metanome.algorithm_integration.result_receiver.FunctionalDependencyResultReceiver;
import de.metanome.algorithm_integration.result_receiver.UniqueColumnCombinationResultReceiver;
import de.metanome.algorithm_integration.results.FunctionalDependency;
import de.metanome.algorithm_integration.results.UniqueColumnCombination;
import de.uni_mannheim.informatik.dws.t2k.utils.StringUtils;
import de.uni_mannheim.informatik.dws.t2k.webtables.Table;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableRow;
import de.uni_mannheim.informatik.dws.tnt.match.metanome.ExcludedColumnsT2TFileInputGenerator;
import de.uni_mannheim.informatik.dws.tnt.match.metanome.T2TFileInputGenerator;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class FunctionalDependencyUtils {

	/**
	 * Calculates the closure of the given subset of columns under the given set of functional dependencies.
	 * 
	 *  
	 * Let H be a heading, let F be a set of FDs with respect to H, and let Z be a subset of H. Then the closure Z+ of Z under F is the maximal subset C of H such that Z → C is implied by the FDs in F.
	 * 
	 * Z+ := Z ;
	 * do "forever" ;
	 * for each FD X → Y in F
	 * do ;
	 * if X is a subset of Z+
	 * then replace Z+ by the union of Z+ and Y ;
	 * end ;
	 * if Z+ did not change on this iteration
	 * then quit ;  // computation complete 
	 * end ;
	 * 
	 * from: "Database Design and Relational Theory", Chapter 7, O'Reilly
	 * 
	 * @param forColumns the subset of columns
	 * @param functionalDependencies the set of functional dependencies
	 * @return Returns a collection of all columns that are in the calculated closure (and are hence determined by the given subset of columns)
	 */
	public static Set<TableColumn> closure(Set<TableColumn> forColumns, Map<Collection<TableColumn>, Collection<TableColumn>> functionalDependencies) {
		
		Set<TableColumn> result = new HashSet<>();
		
		Map<Collection<TableColumn>, Collection<TableColumn>> fds = new HashMap<>(functionalDependencies);
		
		if(functionalDependencies==null) {
			result = null;
		} else {
			result.addAll(forColumns);
			
			int lastCount;
			do {
				lastCount = result.size();
				
//				Iterator<Map.Entry<Collection<TableColumn>, TableColumn>> fdIt = fds.entrySet().iterator();
//				while(fdIt.hasNext()) {
//					Map.Entry<Collection<TableColumn>, TableColumn> fd = fdIt.next();
//					Collection<TableColumn> determinant = fd.getKey();
//					
//					if(result.containsAll(determinant)) {
//						result.add(fd.getValue());
//						
//						fdIt.remove();
//					}
//					
//					// if the closure already spans the full table
//					if(result.size()==maximalClosureSize) {
//						// no need to continue
//						lastCount = result.size();
//						break;
//					}
//				}
				for(Collection<TableColumn> determinant : fds.keySet()) {
					
					if(result.containsAll(determinant)) {
						result.addAll(functionalDependencies.get(determinant));
					}
					
				}
				
			} while(result.size()!=lastCount);
		}
		
		return result;
	}
	
	public static boolean isSuperKey(Set<TableColumn> key, Table t) {
		Set<TableColumn> closure = closure(key, t.getSchema().getFunctionalDependencies());
		
		return closure.equals(new HashSet<>(t.getColumns()));
	}
	
	public static Collection<Collection<TableColumn>> listCandidateKeys(Table t) {
		
		HashSet<Collection<TableColumn>> superKeys = new HashSet<>();
		
		// check all possible superkeys
		// start with table heading and remove single attributes
		// for all sets that are superkeys, keep removing attributes
		
		LinkedList<Collection<TableColumn>> candidates = new LinkedList<>();
		candidates.add(t.getColumns());
		
//		System.out.println(String.format("Superkey: %s", StringUtils.join(Q.project(t.getColumns(), new TableColumn.ColumnHeaderProjection()), ",")));
		
		while(candidates.size()>0) {
			Collection<TableColumn> current = candidates.removeFirst();
			
			boolean anySubKey = false;
			for(TableColumn c : current) {
				Set<TableColumn> sub = new HashSet<>(current);
				sub.remove(c);
				
				if(isSuperKey(sub, t)) {
					// a subset of current is a superkey, so we don't want to output current
					anySubKey = true;
					candidates.add(sub);
					
//					System.out.println(String.format("Superkey: %s", StringUtils.join(Q.project(sub, new TableColumn.ColumnHeaderProjection()), ",")));
				} else {
//					System.out.println(String.format("No Superkey: %s", StringUtils.join(Q.project(sub, new TableColumn.ColumnHeaderProjection()), ",")));
				}
			}
			
			if(!anySubKey) {
				// current is a minimal superkey, so add it to the output
				superKeys.add(current);
				
//				System.out.println(String.format("Minimal Superkey: %s", StringUtils.join(Q.project(current, new TableColumn.ColumnHeaderProjection()), ",")));
			}
			
			
		}
			
		return superKeys;
	}
	
	public static Map<Collection<TableColumn>, Double> enumerateProbableKeys(Table t, Collection<Collection<TableColumn>> candidateKeysToCheck, double minProbability) {
		
		// list all probable keys with their probability
		HashMap<Collection<TableColumn>, Double> probableKeys = new HashMap<>();
		

		LinkedList<Collection<TableColumn>> candidates = new LinkedList<>();
//		candidates.addAll(t.getSchema().getCandidateKeys());
		candidates.addAll(candidateKeysToCheck);
			
//			System.out.println(String.format("Superkey: %s", StringUtils.join(Q.project(t.getColumns(), new TableColumn.ColumnHeaderProjection()), ",")));
			
		while(candidates.size()>0) {
			Collection<TableColumn> current = candidates.removeFirst();

			for(TableColumn c : current) {
				Set<TableColumn> sub = new HashSet<>(current);
				sub.remove(c);
				
				double uniqueness = calculateUniqueness(t, sub);
				
				if(uniqueness >= minProbability) {
					probableKeys.put(sub, uniqueness);
					if(sub.size()>1) {
						candidates.add(sub);
					}
				}
			}
		}	
			
		return probableKeys;
	}
	
	public static boolean isUCC(Table t, Collection<TableColumn> columns) {
		HashSet<String> values = new HashSet<>();
		
		for(TableRow r : t.getRows()) {
			String allValues = StringUtils.join(r.project(columns), "");
			if(values.contains(allValues)) {
				return false;
			}
			values.add(allValues);
		}
		
		return true;
	}
	
	public static double calculateUniqueness(Table t, Collection<TableColumn> columns) {
		HashSet<String> values = new HashSet<>();
		
		for(TableRow r : t.getRows()) {
			String allValues = StringUtils.join(r.project(columns), "");
			values.add(allValues);
		}
		
		return (double)values.size() / (double)t.getRows().size();
	}
	
	public static Collection<Collection<TableColumn>> listCandidateKeysExcludingColumns(Table t, Collection<TableColumn> excludedColumns) {
		
//		System.out.println(String.format("Excluding %s", StringUtils.join(Q.project(excludedColumns, new TableColumn.TableHeaderProjection()), ",")));
		
		HashSet<Collection<TableColumn>> superKeys = new HashSet<>();
		
		// check all possible superkeys
		// start with table heading and remove single attributes
		// for all sets that are superkeys, keep removing attributes
		
		LinkedList<Collection<TableColumn>> candidates = new LinkedList<>();
		Set<TableColumn> heading = new HashSet<>(t.getColumns());
		heading.removeAll(excludedColumns);
		candidates.add(heading);
		
		while(candidates.size()>0) {
			Collection<TableColumn> current = candidates.removeFirst();
			
			boolean anySubKey = false;
			for(TableColumn c : current) {
				Set<TableColumn> sub = new HashSet<>(current);
				sub.remove(c);
				
				// check if it's a super key
				Set<TableColumn> closure = closure(new HashSet<>(sub), t.getSchema().getFunctionalDependencies());
				closure.removeAll(excludedColumns);
				
				if(closure.equals(heading)) {
					// a subset of current is a superkey, so we don't want to output current
					anySubKey = true;
					candidates.add(sub);
				}
			}
			
			if(!anySubKey) {
				
				
				// current is a minimal superkey, so add it to the output
				superKeys.add(current);
			}
			
			
		}
		
		return superKeys;
	}
	
	public static Map<Collection<TableColumn>, Collection<TableColumn>> calculateFunctionalDependencies(final Table t, File tableAsCsv) throws FileNotFoundException, AlgorithmExecutionException {
		HyFD dep = new HyFD();
		final Map<Collection<TableColumn>, Collection<TableColumn>> functionalDependencies = new HashMap<>();
		
		RelationalInputGenerator input = new T2TFileInputGenerator(tableAsCsv);
		dep.setRelationalInputConfigurationValue(HyFD.Identifier.INPUT_GENERATOR.name(), input);
		dep.setResultReceiver(new FunctionalDependencyResultReceiver() {
			
			@Override
			public void receiveResult(FunctionalDependency arg0)
					throws CouldNotReceiveResultException, ColumnNameMismatchException {
				
				synchronized (this) {
					
				
				List<TableColumn> det = new LinkedList<>();
				
				// identify determinant
				for(ColumnIdentifier ci : arg0.getDeterminant().getColumnIdentifiers()) {						    		
					Integer colIdx = Integer.parseInt(ci.getColumnIdentifier());
			
					det.add(t.getSchema().get(colIdx));
				}

				// add dependant
				Collection<TableColumn> dep = null;
				// check if we already have a dependency with the same determinant
				if(functionalDependencies.containsKey(det)) {
					// if so, we add the dependent to the existing dependency
					dep = functionalDependencies.get(det);
				} 
				if(dep==null) {
					// otherwise, we create a new dependency
					dep = new LinkedList<>();
					functionalDependencies.put(det, dep);
				}
				Integer colIdx = Integer.parseInt(arg0.getDependant().getColumnIdentifier());
				dep.add(t.getSchema().get(colIdx));
				
				}
			}
			
			@Override
			public Boolean acceptedResult(FunctionalDependency arg0) {
				return true;
			}
		});
		
		dep.execute();
		
		return functionalDependencies;
	}
	
	public static Collection<Set<TableColumn>> calculateUniqueColumnCombinations(final Table t, File tableAsCsv) throws FileNotFoundException, AlgorithmExecutionException {
		final Collection<Set<TableColumn>> candKeys = new HashSet<>(); //FunctionalDependencyUtils.listCandidateKeys(t);
		
		HyUCC ucc = new HyUCC();
		
		T2TFileInputGenerator input = new T2TFileInputGenerator(tableAsCsv);
		ucc.setRelationalInputConfigurationValue(HyFD.Identifier.INPUT_GENERATOR.name(), input);
		ucc.setResultReceiver(new UniqueColumnCombinationResultReceiver() {
			
			@Override
			public void receiveResult(UniqueColumnCombination arg0)
					throws CouldNotReceiveResultException, ColumnNameMismatchException {
				Set<TableColumn> cand = new HashSet<>();
				
				for(ColumnIdentifier ci : arg0.getColumnCombination().getColumnIdentifiers()) {						    		
					Integer colIdx = Integer.parseInt(ci.getColumnIdentifier());
					cand.add(t.getSchema().get(colIdx));
				}
				
				candKeys.add(cand);
			}
			
			@Override
			public Boolean acceptedResult(UniqueColumnCombination arg0) {
				return true;
			}
		});
		
		ucc.execute();
		
		return candKeys;
	}
	
	public static Collection<Set<TableColumn>> calculateUniqueColumnCombinationsExcludingSpecialColumns(final Table t, File tableAsCsv) throws FileNotFoundException, AlgorithmExecutionException {
		final Collection<Set<TableColumn>> candKeys = new HashSet<>(); //FunctionalDependencyUtils.listCandidateKeys(t);
		
		HyUCC ucc = new HyUCC();
		
		ExcludedColumnsT2TFileInputGenerator inputEx = new ExcludedColumnsT2TFileInputGenerator(tableAsCsv);
		inputEx.setExcludedColumns(Arrays.asList(new Integer[] { 0, 1 }));
		ucc.setRelationalInputConfigurationValue(HyFD.Identifier.INPUT_GENERATOR.name(), inputEx);
		ucc.setResultReceiver(new UniqueColumnCombinationResultReceiver() {
			
			@Override
			public void receiveResult(UniqueColumnCombination arg0)
					throws CouldNotReceiveResultException, ColumnNameMismatchException {
				Set<TableColumn> cand = new HashSet<>();
				
				for(ColumnIdentifier ci : arg0.getColumnCombination().getColumnIdentifiers()) {						    		
					Integer colIdx = Integer.parseInt(ci.getColumnIdentifier()) + 2; // always add 2 as we excluded the first two columns
					cand.add(t.getSchema().get(colIdx));
				}
				
				candKeys.add(cand);
			}
			
			@Override
			public Boolean acceptedResult(UniqueColumnCombination arg0) {
				return true;
			}
		});
		
		ucc.execute();
		
		return candKeys;
	}
}
