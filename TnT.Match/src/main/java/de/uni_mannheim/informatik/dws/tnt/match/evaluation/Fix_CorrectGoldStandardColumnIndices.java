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
package de.uni_mannheim.informatik.dws.tnt.match.evaluation;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class Fix_CorrectGoldStandardColumnIndices {

	public static void main(String[] args) throws IOException {
		
		N2NGoldStandard gs = new N2NGoldStandard();
		gs.loadFromTSV(new File(args[0]));
		
		Map<Set<String>, String> existing = gs.getCorrespondenceClusters();
		Map<Set<String>, String> corrected = new HashMap<>();
		
		for(Set<String> clu : existing.keySet()) {
			
			Set<String> correctedClu = new HashSet<>();
			
			for(String elem : clu) {
				String[] values = elem.split(";");
				
				int index = Integer.parseInt(values[1]) - 2;
				
				String correctedElem = String.format("%s;%d;%s", values[0], index, values[2]);
				
				correctedClu.add(correctedElem);
			}
			
			
			corrected.put(correctedClu, existing.get(clu));
		}
		
		gs.getCorrespondenceClusters().clear();
		for(Set<String> clu : corrected.keySet()) {
			gs.getCorrespondenceClusters().put(clu, corrected.get(clu));
		}
		
		gs.writeToTSV(new File(args[0] + "_corrected.tsv"));
	}
	
}
