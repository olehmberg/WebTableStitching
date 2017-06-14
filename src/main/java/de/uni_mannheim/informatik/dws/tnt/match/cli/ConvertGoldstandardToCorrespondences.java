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
package de.uni_mannheim.informatik.dws.tnt.match.cli;

import java.io.File;
import java.io.IOException;

import com.beust.jcommander.Parameter;

import de.uni_mannheim.informatik.dws.tnt.match.evaluation.N2NGoldStandard;
import de.uni_mannheim.informatik.dws.winter.utils.Executable;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class ConvertGoldstandardToCorrespondences extends Executable {

	@Parameter(names = "-n2n",required=true)
	private String n2nLocation;
	
	@Parameter(names = "-result",required=true)
	private String resultLocation;
	
	public static void main(String[] args) throws IOException {
		
		ConvertGoldstandardToCorrespondences conv = new ConvertGoldstandardToCorrespondences();
		
		if(conv.parseCommandLine(ConvertGoldstandardToCorrespondences.class, args)) {
			
			conv.run();
			
		}
		
	}
	
	
	public void run() throws IOException {

		N2NGoldStandard n2n = new N2NGoldStandard();
		n2n.loadFromTSV(new File(n2nLocation));
		n2n.convertToCorrespondenceBasedGoldStandard(new File(resultLocation));
		
	}
}
