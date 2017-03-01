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
package de.uni_mannheim.informatik.wdi.matching.v2;

import java.io.Serializable;

import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.DefaultRecord;
import de.uni_mannheim.informatik.wdi.model.FeatureVectorDataSet;
import de.uni_mannheim.informatik.wdi.model.ResultSet;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public abstract class MatchingRule<RecordTypeA, RecordTypeB> implements Serializable {

	private static final long serialVersionUID = 1L;


	public abstract ResultSet<Correspondence<RecordTypeA, RecordTypeB>> apply(RecordTypeA record1,
			RecordTypeA record2, ResultSet<Correspondence<RecordTypeB, RecordTypeA>> typeBCorrespondences);


	public abstract DefaultRecord generateFeatures(RecordTypeA record1,
			RecordTypeA record2, ResultSet<Correspondence<RecordTypeB, RecordTypeA>> typeBCorrespondences, FeatureVectorDataSet features);
}
