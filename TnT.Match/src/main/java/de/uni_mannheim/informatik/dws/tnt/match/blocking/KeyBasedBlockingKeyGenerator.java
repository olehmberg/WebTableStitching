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
package de.uni_mannheim.informatik.dws.tnt.match.blocking;

import java.util.Set;

import de.uni_mannheim.informatik.wdi.model.Matchable;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
//TODO this class is very general... either move to the general framework or integrate in the extending class (WebTableKeyBlockingKeyGenerator)
public abstract class KeyBasedBlockingKeyGenerator<RecordType extends Matchable, KeyType> {

	public static enum AmbiguityAvoidance {
		None,
		Local,
		Global
	}
	
	private AmbiguityAvoidance ambiguityAvoidanceMode = AmbiguityAvoidance.None;
	private Set<Object> ambiguousValues;
	
	/**
	 * @return the ambiguityAvoidanceMode
	 */
	public AmbiguityAvoidance getAmbiguityAvoidanceMode() {
		return ambiguityAvoidanceMode;
	}
	
	/**
	 * @param ambiguityAvoidanceMode the ambiguityAvoidanceMode to set
	 */
	public void setAmbiguityAvoidanceMode(AmbiguityAvoidance ambiguityAvoidanceMode) {
		this.ambiguityAvoidanceMode = ambiguityAvoidanceMode;
	}
	
	/**
	 * @return the ambiguousValues
	 */
	public Set<Object> getAmbiguousValues() {
		return ambiguousValues;
	}
	
	/**
	 * @param ambiguousValues the ambiguousValues to set
	 */
	public void setAmbiguousValues(Set<Object> ambiguousValues) {
		this.ambiguousValues = ambiguousValues;
	}
	
	public abstract Object[] getKeyValues(RecordType record, KeyType key);
	
	public abstract String getBlockingKey(RecordType record, KeyType key);

}
