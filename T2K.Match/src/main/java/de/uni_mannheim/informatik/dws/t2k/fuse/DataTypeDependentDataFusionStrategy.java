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
package de.uni_mannheim.informatik.dws.t2k.fuse;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import de.uni_mannheim.informatik.dws.t2k.datatypes.DataType;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.wdi.datafusion.AttributeFuser;
import de.uni_mannheim.informatik.wdi.datafusion.AttributeFusionTask;
import de.uni_mannheim.informatik.wdi.datafusion.DataFusionStrategy;
import de.uni_mannheim.informatik.wdi.datafusion.EvaluationRule;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.FusableFactory;
import de.uni_mannheim.informatik.wdi.model.RecordGroup;
import de.uni_mannheim.informatik.wdi.model.ResultSet;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class DataTypeDependentDataFusionStrategy extends DataFusionStrategy<MatchableTableRow, MatchableTableColumn> {

	private Map<DataType, AttributeFuser<MatchableTableRow, MatchableTableColumn>> dataTypeFusers;
	private Map<DataType, EvaluationRule<MatchableTableRow, MatchableTableColumn>> dataTypeEvaluationRules;
	
	public DataTypeDependentDataFusionStrategy(
			FusableFactory<MatchableTableRow, MatchableTableColumn> factory) {
		super(factory);
		dataTypeFusers = new HashMap<>();
		dataTypeEvaluationRules = new HashMap<>();
	}
	
	public void addFuserForDataType(DataType type, AttributeFuser<MatchableTableRow, MatchableTableColumn> fuser, EvaluationRule<MatchableTableRow, MatchableTableColumn> rule) {
		dataTypeFusers.put(type, fuser);
		dataTypeEvaluationRules.put(type, rule);
	}
	
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.datafusion.DataFusionStrategy#getAttributeFusers(de.uni_mannheim.informatik.wdi.model.ResultSet)
	 */
	@Override
	public List<AttributeFusionTask<MatchableTableRow, MatchableTableColumn>> getAttributeFusers(
			RecordGroup<MatchableTableRow, MatchableTableColumn> group, ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences) {
		
		List<AttributeFusionTask<MatchableTableRow, MatchableTableColumn>> fusers = new ArrayList<>();

		Set<Integer> tablesInGroup = new HashSet<>();
		
		if(group!=null) {
			for(MatchableTableRow r : group.getRecords()) {
				tablesInGroup.add(r.getTableId());
			}
		}
		
		// collect all correspondences for each element of the target schema 
		Map<String, ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>>> byTargetSchema = new HashMap<>();
		Map<String, MatchableTableColumn> targetElements = new HashMap<>();
		
		for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : schemaCorrespondences.get()) {
			
			// only care about the correspondence if its from any of our records' table
			if(group!=null && tablesInGroup.contains(cor.getFirstRecord().getTableId())) {
			
				ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> cors = byTargetSchema.get(cor.getSecondRecord().getIdentifier());
				
				if(cors==null) {
					cors = new ResultSet<>();
					byTargetSchema.put(cor.getSecondRecord().getIdentifier(), cors);
					targetElements.put(cor.getSecondRecord().getIdentifier(), cor.getSecondRecord());
				}
			
				cors.add(cor);
			}
		}
		
		for(String id : byTargetSchema.keySet()) {
			MatchableTableColumn elem = targetElements.get(id);
			AttributeFusionTask<MatchableTableRow, MatchableTableColumn> t = new AttributeFusionTask<>();
			t.setSchemaElement(elem);
			t.setFuser(dataTypeFusers.get(elem.getType()));
			t.setCorrespondences(byTargetSchema.get(id));
			t.setEvaluationRule(dataTypeEvaluationRules.get(elem.getType()));
			
			if(byTargetSchema.get(id)==null) {
				System.out.println("No Correspondences!");
			}
			
//			for(MatchableTableRow row : group.getRecords()) {
//				Correspondence<MatchableTableColumn> cor = group.getSchemaCorrespondenceForRecord(row, byTargetSchema.get(id), elem);
//				if(cor!=null && row.hasValue(elem)) {
//					MatchableTableColumn col = cor.getFirstRecord();
//					if(row.getType(col.getColumnIndex())!=elem.getType()) {
//						System.out.println(String.format("Type of record %s (%s) [%s] does not match correspondence %d -> %s (%s)", row.getIdentifier(), row.getType(col.getColumnIndex()), row.get(col.getColumnIndex()), col.getColumnIndex(), elem.getIdentifier(), elem.getType()));
//					}
//					else {
//						try {
//							switch(elem.getType()) {
//							case string:
//								String s = (String)row.get(col.getColumnIndex());
//							case bool:
//								break;
//							case coordinate:
//								break;
//							case date:
//								DateTime dt = (DateTime)row.get(col.getColumnIndex());
//								break;
//							case link:
//								break;
//							case list:
//								break;
//							case numeric:
//								Double d = (Double)row.get(col.getColumnIndex());
//								break;
//							case unit:
//								break;
//							case unknown:
//								break;
//							default:
//								break;
//							}
//						} catch(Exception e) {
//							System.out.println(String.format("Type of record %s (%s) [%s] does not match correspondence %d -> %s (%s)", row.getIdentifier(), row.getType(col.getColumnIndex()), row.get(col.getColumnIndex()), col.getColumnIndex(), elem.getIdentifier(), elem.getType()));
//						}
//					}
//				}
//			}
			
			
			fusers.add(t);
		}
		
		return fusers;
	}
}
