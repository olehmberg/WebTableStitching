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
package de.uni_mannheim.informatik.dws.t2k.utils.data.string;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class BagOfWords {

	private List<String> words = new ArrayList<String>();
	private Map<String, Integer> wordIndex = new HashMap<>(); 
	
	public int getNumDimensions() {
		return words.size();
	}
	
	public void index(Collection<String> values) {
		for(String s : values) {
			if(!wordIndex.containsKey(s)) {
				wordIndex.put(s, words.size());
				words.add(s);
			}
		}
	}
	
	public boolean[] vectoriseBoolean(Collection<String> values) {
		boolean[] vector = new boolean[words.size()];
		
		for(String s : values) {
			Integer idx = wordIndex.get(s);
			
			if(idx!=null) {
				vector[idx]=true;
			}
		}
		
		return vector;
	}
}
