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
package de.uni_mannheim.informatik.wdi.datafusion.conflictresolution.string;

import java.util.Collection;

import de.uni_mannheim.informatik.wdi.datafusion.conflictresolution.ConflictResolutionFunction;
import de.uni_mannheim.informatik.wdi.model.Fusable;
import de.uni_mannheim.informatik.wdi.model.FusableValue;
import de.uni_mannheim.informatik.wdi.model.FusedValue;
import de.uni_mannheim.informatik.wdi.model.Matchable;

/**
 * Shortest string {@link ConflictResolutionFunction}: Returns the shortest string value
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 * @param <RecordType>
 */
public class ShortestString<RecordType extends Matchable & Fusable<SchemaElementType>, SchemaElementType> extends ConflictResolutionFunction<String, RecordType, SchemaElementType> {

	@Override
	public FusedValue<String, RecordType, SchemaElementType> resolveConflict(Collection<FusableValue<String, RecordType, SchemaElementType>> values) {
		FusableValue<String, RecordType, SchemaElementType> shortest = null;
		
		for(FusableValue<String, RecordType, SchemaElementType> value : values) {
			if(shortest == null || value.getValue().length()<shortest.getValue().length()) {
				shortest = value;
			}
		}
		
		return new FusedValue<>(shortest);
	}

}
