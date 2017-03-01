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
import de.uni_mannheim.informatik.wdi.model.DataSet;
import de.uni_mannheim.informatik.wdi.model.Matchable;
import de.uni_mannheim.informatik.wdi.model.ResultSet;

/**
 * super class for schema matching rules.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public abstract class SchemaMatchingRule<RecordType, SchemaElementType extends Matchable, SchemaElementMetaDataType> implements Serializable {

	private static final long serialVersionUID = 1L;
	private double finalThreshold;

	public double getFinalThreshold() {
		return finalThreshold;
	}

	public void setFinalThreshold(double finalThreshold) {
		this.finalThreshold = finalThreshold;
	}

	public SchemaMatchingRule(double finalThreshold) {
		this.finalThreshold = finalThreshold;
	}

	
	public abstract ResultSet<Correspondence<SchemaElementType, RecordType>> apply(
			DataSet<SchemaElementType, SchemaElementMetaDataType> schema1, 
			DataSet<SchemaElementType, SchemaElementMetaDataType> schema2, 
			Correspondence<RecordType, SchemaElementType> correspondence);
//	
//	public abstract ResultSet<Pair<Correspondence<RecordType, SchemaElementType>,  ResultSet<Correspondence<SchemaElementType, RecordType>>>> apply(
//			DataSet<SchemaElementType, SchemaElementMetaDataType> schema1, 
//			DataSet<SchemaElementType, SchemaElementMetaDataType> schema2,
//			ResultSet<Correspondence<RecordType, SchemaElementType>> instanceCorrespondences);

}
