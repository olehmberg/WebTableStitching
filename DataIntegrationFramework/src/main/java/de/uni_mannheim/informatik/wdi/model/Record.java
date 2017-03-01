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
package de.uni_mannheim.informatik.wdi.model;

import java.io.Serializable;

/**
 * The super class for all models, should be extended by all model classes.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 * 
 */
public abstract class Record<SchemaElementType> implements Matchable, Fusable<SchemaElementType>, Serializable {

	private static final long serialVersionUID = 1L;
	protected String id;
	protected String provenance;

	public Record() {
		
	}
	
	public Record(String identifier, String provenance) {
		id = identifier;
		this.provenance = provenance;
	}

	@Override
	public String getIdentifier() {
		return id;
	}

	@Override
	public String getProvenance() {
		return provenance;
	}

//	@Override
//	public Collection<String> getAttributeNames() {
//		return new LinkedList<>();
//	}

	@Override
	public abstract boolean hasValue(SchemaElementType attribute);
}
