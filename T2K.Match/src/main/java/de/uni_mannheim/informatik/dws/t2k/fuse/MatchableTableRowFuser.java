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

import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.wdi.datafusion.AttributeValueFuser;
import de.uni_mannheim.informatik.wdi.datafusion.conflictresolution.ConflictResolutionFunction;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.FusedValue;
import de.uni_mannheim.informatik.wdi.model.RecordGroup;
import de.uni_mannheim.informatik.wdi.model.ResultSet;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class MatchableTableRowFuser<ValueType> extends AttributeValueFuser<ValueType, MatchableTableRow, MatchableTableColumn> {
	
	/**
	 * @param conflictResolution
	 */
	public MatchableTableRowFuser(
			ConflictResolutionFunction<ValueType, MatchableTableRow, MatchableTableColumn> conflictResolution) {
		super(conflictResolution);
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.datafusion.AttributeValueFuser#getValue(de.uni_mannheim.informatik.wdi.model.Matchable)
	 */
	@SuppressWarnings("unchecked")
	@Override
	protected ValueType getValue(MatchableTableRow record, Correspondence<MatchableTableColumn, MatchableTableRow> correspondence) {
		return (ValueType) record.get(correspondence.getFirstRecord().getColumnIndex());
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.datafusion.AttributeFuser#fuse(de.uni_mannheim.informatik.wdi.model.RecordGroup, de.uni_mannheim.informatik.wdi.model.Matchable, de.uni_mannheim.informatik.wdi.model.ResultSet)
	 */
	@Override
	public void fuse(
			RecordGroup<MatchableTableRow, MatchableTableColumn> group,
			MatchableTableRow fusedRecord,
			ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences, MatchableTableColumn column) {
		
		FusedValue<ValueType, MatchableTableRow, MatchableTableColumn> value = getFusedValue(group, schemaCorrespondences, column);
		
		// get the first schema correspondence (all correspondences point to the same DBpedia property, so it doesn't really matter which one we use)
		Correspondence<MatchableTableColumn, MatchableTableRow> schemaCorrespondence = schemaCorrespondences.get().iterator().next();
		
		//TODO find out how to set values of a MatchableTableRow without re-creating all arrays every time ...
		fusedRecord.set(schemaCorrespondence.getSecondRecord().getColumnIndex(), value.getValue());
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.datafusion.AttributeFuser#hasValue(de.uni_mannheim.informatik.wdi.model.Matchable, de.uni_mannheim.informatik.wdi.model.Correspondence)
	 */
	@Override
	public boolean hasValue(MatchableTableRow record,
			Correspondence<MatchableTableColumn, MatchableTableRow> correspondence) {
		if(correspondence==null) {
			return false;
		} else {
			return record.hasValue(correspondence.getFirstRecord());
		}
	}
}
