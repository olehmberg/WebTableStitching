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

import java.io.Serializable;

import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.Matchable;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;

/**
 * Super class for schema matching rules with voting.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 * 
 * @param <RecordType>
 */
public abstract class SchemaMatchingRuleWithVoting<RecordType, SchemaElementType extends Matchable, SchemaElementMetaDataType>  implements Serializable {
//extends SchemaMatchingRule<RecordType, SchemaElementType, SchemaElementMetaDataType>
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private double finalThreshold;

	public double getFinalThreshold() {
		return finalThreshold;
	}

	public void setFinalThreshold(double finalThreshold) {
		this.finalThreshold = finalThreshold;
	}

	public SchemaMatchingRuleWithVoting(double finalThreshold) {
		this.finalThreshold = finalThreshold;
	}
	
	public abstract Correspondence<SchemaElementType, RecordType> apply(
			SchemaElementType schemaElement1, 
			SchemaElementType schemaElement2, 
			Correspondence<RecordType, SchemaElementType> correspondence);
	
	/***
	 * 
	 * @param results the output of calling apply() for all instance correspondences
	 * @param numVotes the number of instance correspondences
	 * @return
	 */
	public abstract ResultSet<Correspondence<SchemaElementType, RecordType>> aggregate(ResultSet<Pair<Correspondence<RecordType, SchemaElementType>,Correspondence<SchemaElementType, RecordType>>> results, int numVotes, DataProcessingEngine processingEngine);
}
