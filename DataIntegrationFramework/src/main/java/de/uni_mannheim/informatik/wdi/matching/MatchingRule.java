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
package de.uni_mannheim.informatik.wdi.matching;

import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.DefaultRecord;
import de.uni_mannheim.informatik.wdi.model.FeatureVectorDataSet;
import de.uni_mannheim.informatik.wdi.model.ResultSet;

/**
 * Super class for all matching rules.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 * 
 * @param <RecordType>
 */
public abstract class MatchingRule<RecordType, SchemaElementType> extends Comparator<RecordType,SchemaElementType> {

	private static final long serialVersionUID = 1L;
	private double finalThreshold;

	public double getFinalThreshold() {
		return finalThreshold;
	}

	public void setFinalThreshold(double finalThreshold) {
		this.finalThreshold = finalThreshold;
	}

	public MatchingRule(double finalThreshold) {
		this.finalThreshold = finalThreshold;
	}

	protected Correspondence<RecordType, SchemaElementType> createCorrespondence(RecordType record1,
			RecordType record2, ResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences, double similarity) {
		return new Correspondence<>(record1, record2, similarity, schemaCorrespondences);
	}
	
	public abstract Correspondence<RecordType, SchemaElementType> apply(RecordType record1,
			RecordType record2, ResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences); //{
//		double similarity = compare(record1, record2, schemaCorrespondences);
//
//		if (similarity >= getFinalThreshold() && similarity > 0.0) {
//			return createCorrespondence(record1, record2, schemaCorrespondences, similarity);
//		} else {
//			return null;
//		}
//	}
	
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.matching.Comparator#compare(java.lang.Object, java.lang.Object, de.uni_mannheim.informatik.wdi.model.Correspondence)
	 */
	@Override
	public double compare(RecordType record1, RecordType record2,
			Correspondence<SchemaElementType, RecordType> schemaCorrespondences) {
		return 0;
	}

	public abstract DefaultRecord generateFeatures(RecordType record1,
			RecordType record2, ResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences, FeatureVectorDataSet features);
}
