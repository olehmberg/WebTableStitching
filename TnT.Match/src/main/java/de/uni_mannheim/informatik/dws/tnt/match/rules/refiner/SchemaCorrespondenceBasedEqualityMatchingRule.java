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
package de.uni_mannheim.informatik.dws.tnt.match.rules.refiner;

import java.util.HashSet;

import de.uni_mannheim.informatik.dws.t2k.utils.StringUtils;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.wdi.matching.MatchingRule;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.DefaultRecord;
import de.uni_mannheim.informatik.wdi.model.FeatureVectorDataSet;
import de.uni_mannheim.informatik.wdi.model.ResultSet;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class SchemaCorrespondenceBasedEqualityMatchingRule extends MatchingRule<MatchableTableRow, MatchableTableColumn> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private boolean allowMissingValues = false;
	/**
	 * @param allowMissingValues the allowMissingValues to set
	 */
	public void setAllowMissingValues(boolean allowMissingValues) {
		this.allowMissingValues = allowMissingValues;
	}
	
	/**
	 * @param finalThreshold
	 */
	public SchemaCorrespondenceBasedEqualityMatchingRule(double finalThreshold) {
		super(finalThreshold);
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.matching.MatchingRule#apply(java.lang.Object, java.lang.Object, de.uni_mannheim.informatik.wdi.model.ResultSet)
	 */
	@Override
	public Correspondence<MatchableTableRow, MatchableTableColumn> apply(
			MatchableTableRow record1,
			MatchableTableRow record2,
			ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences) {
		
		double sum = 0.0;
		double count = 0.0;
		ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> causalCorrespondences = new ResultSet<>();
		HashSet<String> mismatches = new HashSet<>();
		HashSet<String> matches = new HashSet<>();
		
		for(Correspondence<MatchableTableColumn, MatchableTableRow> cor : schemaCorrespondences.get()) {
			Object v1 = record1.get(cor.getFirstRecord().getColumnIndex());
			Object v2 = record2.get(cor.getSecondRecord().getColumnIndex());
			
			//if(v1==null && v2==null || (v1!=null && v1.equals(v2))) {
			if(areEqual(v1, v2)) {
				sum+=1.0;
				causalCorrespondences.add(cor);
				matches.add(String.format("%s==%s", v1, v2));
			} else {
				mismatches.add(String.format("%s!=%s", v1, v2));
			}
			
			count += 1.0;
		}
		
		double sim = sum/count;
		
		if(sim>=getFinalThreshold()) {
//			System.out.println(String.format("Correct link (%f): %s", sim, StringUtils.join(matches, " / ")));
//			System.out.println("\t" + record1.format(20));
//			System.out.println("\t" + record2.format(20));
			return new Correspondence<MatchableTableRow, MatchableTableColumn>(record1, record2, sim, causalCorrespondences);
		} else {
			System.out.println(String.format("Incorrect link (%f): %s", sim, StringUtils.join(mismatches, " / ")));
			System.out.println("\t" + record1.format(20));
			System.out.println("\t" + record2.format(20));
			return null;	
		}
	}
	
	private boolean areEqual(Object v1, Object v2) {
		if(allowMissingValues) {
			return (v1==null || v2==null) || v1.equals(v2);
		} else {
			return v1==null && v2==null || (v1!=null && v1.equals(v2));
		}
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.matching.MatchingRule#generateFeatures(java.lang.Object, java.lang.Object, de.uni_mannheim.informatik.wdi.model.ResultSet, de.uni_mannheim.informatik.wdi.model.FeatureVectorDataSet)
	 */
	@Override
	public DefaultRecord generateFeatures(
			MatchableTableRow record1,
			MatchableTableRow record2,
			ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences,
			FeatureVectorDataSet features) {
		// TODO Auto-generated method stub
		return null;
	}



}
