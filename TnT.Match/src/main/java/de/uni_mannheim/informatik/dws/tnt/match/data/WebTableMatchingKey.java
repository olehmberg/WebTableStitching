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
package de.uni_mannheim.informatik.dws.tnt.match.data;

import java.util.HashSet;
import java.util.Set;

import de.uni_mannheim.informatik.dws.t2k.utils.StringUtils;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableColumn;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class WebTableMatchingKey {

	private Set<TableColumn> firstKey;
	private Set<TableColumn> secondKey;
	
	/**
	 * @return the firstKey
	 */
	public Set<TableColumn> getFirst() {
		return firstKey;
	}
	
	
	/**
	 * @return the secondKey
	 */
	public Set<TableColumn> getSecond() {
		return secondKey;
	}
	
	public WebTableMatchingKey(Set<TableColumn> firstKey, Set<TableColumn> secondKey) {
		this.firstKey = firstKey;
		this.secondKey = secondKey;
	}
	
	public WebTableMatchingKey invert() {
		return new WebTableMatchingKey(new HashSet<>(secondKey), new HashSet<>(firstKey));
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof WebTableMatchingKey) {
			WebTableMatchingKey k = (WebTableMatchingKey)obj;
			
			return getFirst().equals(k.getFirst()) && getSecond().equals(k.getSecond());
		} else {
			return super.equals(obj);
		}
	}


	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return 997 * ((int)getFirst().hashCode()) ^ 991 * ((int)getSecond().hashCode());
//		return (int)getFirst().hashCode() + (int)getSecond().hashCode()
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.format("{%s}<->{%s}", 
				StringUtils.join(Q.project(firstKey, new TableColumn.ColumnHeaderProjection()), ","),
				StringUtils.join(Q.project(secondKey, new TableColumn.ColumnHeaderProjection()), ","));
	}

}
