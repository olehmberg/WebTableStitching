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
package de.uni_mannheim.informatik.dws.tnt.match.rules.MultiLanguageUnionTables;

import java.util.Comparator;
import java.util.List;
import java.util.regex.Pattern;

import de.uni_mannheim.informatik.dws.t2k.utils.data.Distribution;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.tnt.match.ContextColumns;
import de.uni_mannheim.informatik.dws.tnt.match.SpecialColumns;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTable;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.DataSet;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;
import de.uni_mannheim.informatik.wdi.processing.DatasetIterator;
import de.uni_mannheim.informatik.wdi.processing.Group;
import de.uni_mannheim.informatik.wdi.processing.RecordKeyValueMapper;
import de.uni_mannheim.informatik.wdi.processing.RecordMapper;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class MultiLanguageUnionTableBlocker {
	
	protected static final Pattern nonCharactersOnly = Pattern.compile("[\\W\\d]+");
	
	public ResultSet<Correspondence<MatchableTable, MatchableTableColumn>> runBlocking(
			DataSet<MatchableTable, MatchableTableColumn> dataset,
			boolean isSymmetric,
			ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences,
			DataProcessingEngine engine) {
		
		// block tables by 
    	// 1) same position in DOM tree (use table index as substitute)
    	// 2) same number of columns (including those generated from the URI)
    	// 3) no/only coincidental overlap in column headers (TODO define measure)
    	// -- look at the co-occurrence distribution of column headers among all selected tables)
    	// -- if we have multiple languages there should be no headers to co-occur for too many different languages..
		
		
		RecordKeyValueMapper<List<Integer>, MatchableTable, MatchableTable> groupBy = new RecordKeyValueMapper<List<Integer>, MatchableTable, MatchableTable>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(MatchableTable record, DatasetIterator<Pair<List<Integer>, MatchableTable>> resultCollector) {
				
				int columns = 0;
				int context = 0;
				
				for(MatchableTableColumn c : record.getSchema()) {
					if(SpecialColumns.isSpecialColumn(c)) {
						// ignore
					} else if(ContextColumns.isContextColumn(c)) {
						context++;
					} else {
						columns++;
					}
				}
				
				resultCollector.next(new Pair<List<Integer>, MatchableTable>(Q.toList(record.getTableIndex(), context, columns), record));
			}
		};
		ResultSet<Group<List<Integer>, MatchableTable>> grouped = engine.groupRecords(dataset, groupBy);
		
		RecordMapper<Group<List<Integer>, MatchableTable>, Correspondence<MatchableTable, MatchableTableColumn>> transformation = new RecordMapper<Group<List<Integer>,MatchableTable>, Correspondence<MatchableTable,MatchableTableColumn>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(Group<List<Integer>, MatchableTable> record,
					DatasetIterator<Correspondence<MatchableTable, MatchableTableColumn>> resultCollector) {

				if(record.getRecords().size()>1) {
					// calculate the frequency distribution of column headers
					
					Distribution<String> headerDistribution = new Distribution<>();
					
					for(MatchableTable t : record.getRecords().get()) {
						
						for(MatchableTableColumn c : t.getSchema()) {
							
							if(!SpecialColumns.isSpecialColumn(c) && !ContextColumns.isContextColumn(c) && !c.getHeader().equals("null") && !nonCharactersOnly.matcher(c.getHeader()).matches()) {
							
								String header = String.format("[%d]%s", c.getColumnIndex(), c.getHeader());
								
								headerDistribution.add(header);
							
							}
							
						}
						
					}
					
					// check if the distribution matches that one of a set of union tables in different languages
					int numTables = record.getRecords().size();
					boolean ok = true;
					
					for(String element : headerDistribution.getElements()) {
						if(headerDistribution.getFrequency(element)==numTables) {
							// this header appear in all tables (i.e. all languages), which is very unlikely if we really have different languages
							ok = false;
						}
					}
					
					if(ok) {
						List<MatchableTable> tables = Q.sort(record.getRecords().get(), new Comparator<MatchableTable>(){
	
							@Override
							public int compare(MatchableTable o1, MatchableTable o2) {
								return Integer.compare(o1.getTableId(), o2.getTableId());
							}});
						
						
						for(int i = 0; i < tables.size(); i++) {
							MatchableTable t1 = tables.get(i);
							for(int j = i+1; j < tables.size(); j++) {
								MatchableTable t2 = tables.get(j);
								
								resultCollector.next(new Correspondence<MatchableTable, MatchableTableColumn>(t1, t2, 1.0, null));
							}
						}
						
						System.out.println("A cluster of multi-language union tables: " + record.getKey());
						for(MatchableTable t : record.getRecords().get()) {
							System.out.println(String.format("\t{#%d}\t%s", t.getTableId(), Q.toList(t.getSchema())));
						}
					} else {
						System.out.println("Not a cluster of multi-language union tables:"  + record.getKey());
						for(MatchableTable t : record.getRecords().get()) {
							System.out.println(String.format("\t{#%d}\t%s", t.getTableId(), Q.toList(t.getSchema())));
						}
					}
				}
			}
		};
		return engine.transform(grouped, transformation);
	}
	
}
