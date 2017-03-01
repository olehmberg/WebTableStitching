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
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.beust.jcommander.Parameter;

import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.t2k.webtables.Table;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.t2k.webtables.parsers.JsonTableParser;
import de.uni_mannheim.informatik.dws.t2k.webtables.writers.CSVTableWriter;
import de.uni_mannheim.informatik.dws.t2k.webtables.writers.JsonTableWriter;
import de.uni_mannheim.informatik.dws.tnt.match.ContextColumns;
import de.uni_mannheim.informatik.dws.tnt.match.TnTTask;
import de.uni_mannheim.informatik.dws.tnt.match.dependencies.FunctionalDependencyUtils;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.model.Correspondence.RecordId;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class AttributeCategorisation extends TnTTask {

			@Parameter(names = "-web")
			private String webLocation;
			
			@Parameter(names = "-correspondences", required=true)
			private String correspondencesLocation;

			@Parameter(names = "-removeContext")
			private boolean removeContext = false;
			
			public static void main(String[] args) throws Exception {
				AttributeCategorisation task = new AttributeCategorisation();
				
				if(task.parseCommandLine(EntityLabelDetection.class, args)) {
					task.initialise();
					task.match();
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
				File[] files;
				
				if(webFile.isDirectory()) {
					files = webFile.listFiles();
				} else {
					files = new File[] { webFile };
				}
				
				JsonTableParser p = new JsonTableParser();
				p.setConvertValues(false);
				
				ResultSet<Correspondence<RecordId, RecordId>> cors = Correspondence.loadFromCsv(new File(correspondencesLocation));
				
				CSVTableWriter w = new CSVTableWriter();
				
				for(File f : files) {
				
					Table t = p.parseTable(f);
					
					if(t.hasKey()) {
						
						if(removeContext) {
							Table noContext = t.project(Q.where(t.getColumns(), new ContextColumns.IsNoContextColumnPredicate()));
							noContext.deduplicate(noContext.getColumns());
							int numContextColumns = t.getColumns().size() - noContext.getColumns().size();
							
							if(noContext.hasKey()) {
								System.out.println(String.format("Attribute categorisation for '%s' (%d original tables), entity label: %s", f.getName(), getOriginalTables(t).size(), noContext.getKey()));
		
								File tableAsCsv = w.write(noContext, new File("attributecategorisation_temp.csv"));
								
								noContext.getSchema().setFunctionalDependencies(FunctionalDependencyUtils.calculateFunctionalDependencies(noContext, tableAsCsv));
								
								Set<TableColumn> mappedColumns = new HashSet<>();
								
								for(Correspondence<RecordId, RecordId> cor : cors.get()) {
									TableColumn c = t.getSchema().getRecord(cor.getFirstRecord().getIdentifier());
									
									if(c!=null) {
										mappedColumns.add(noContext.getSchema().get(c.getColumnIndex() - numContextColumns));
									}
								}
								
								// attribute is 
								// - independent, if no determinant contains the entity label column
								// - binary, if at least one determinant contains the entity label column and all other attributes in the determinant are mapped
								// - n-ary, if all determinants which contain the entity label column also contain unmapped attributes
								
								for(TableColumn c : noContext.getColumns()) {
									
									System.out.println(String.format("\t%s", c));
									ColumnCategory cat = categoriseColumn(c, noContext, mappedColumns);
									System.out.println(String.format("\t->%s", cat));
									
								}
							}
						} else {
							System.out.println(String.format("Attribute categorisation for '%s' (%d original tables), entity label: %s", f.getName(), getOriginalTables(t).size(), t.getKey()));
							
							
							Set<TableColumn> mappedColumns = new HashSet<>();
							
							for(Correspondence<RecordId, RecordId> cor : cors.get()) {
								TableColumn c = t.getSchema().getRecord(cor.getFirstRecord().getIdentifier());
								
								if(c!=null) {
									mappedColumns.add(c);
								}
							}
							
							// attribute is 
							// - independent, if no determinant contains the entity label column
							// - binary, if at least one determinant contains the entity label column and all other attributes in the determinant are mapped
							// - n-ary, if all determinants which contain the entity label column also contain unmapped attributes
							
							for(TableColumn c : t.getColumns()) {
								
								System.out.println(String.format("\t%s", c));
								ColumnCategory cat = categoriseColumn(c, t, mappedColumns);
								System.out.println(String.format("\t->%s", cat));
								
							}
						}
						
					} else {
						System.out.println("no entity label");
					}
				}
				
			}

			public static enum ColumnCategory {
				Binary,
				NAry,
				Independent,
				Mapped
			}
			
			protected ColumnCategory categoriseColumn(TableColumn c, Table t, Set<TableColumn> mappedColumns) {
				TableColumn entityLabel = t.getKey();
				
				if(c.equals(entityLabel) || mappedColumns.contains(c)) {
					return ColumnCategory.Mapped;
				} else {
					// go through all functional dependencies
					for(Collection<TableColumn> det : t.getSchema().getFunctionalDependencies().keySet()) {
						
						Collection<TableColumn> dep = t.getSchema().getFunctionalDependencies().get(det);
						
						// check if the dependent contains the column and if the determinant contains the entity label
						if(dep.contains(c) && det.contains(entityLabel)) {
							
							boolean isBinaryRelation = true;
							
							// if yes, check the other columns in the determinant
							for(TableColumn detCol : det) {
								
								if(!detCol.equals(t.getKey()) && !mappedColumns.contains(detCol)) {
									
									System.out.println(String.format("\t\t%s: %s not mapped!", det, detCol));
									
									// if a column in the determinant is not the entity label and not mapped, this determinant does not indicate a binary relation
									isBinaryRelation = false;
									break;
								}
								
							}
							
							if(isBinaryRelation) {
								return ColumnCategory.Binary;
							}
						}
					}

					// if no determinant indicated a binary relation, this is an n-ary relation
					return ColumnCategory.NAry;
				}
			}
			
			private Set<String> getOriginalTables(Table t) {
				
				Set<String> tbls = new HashSet<>();
				
				for(TableColumn c : t.getColumns()) {
					for(String prov : c.getProvenance()) {
						
						tbls.add(prov.split(";")[0]);
						
					}
				}
				
				return tbls;
			}
}
