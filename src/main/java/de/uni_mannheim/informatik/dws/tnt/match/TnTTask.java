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
package de.uni_mannheim.informatik.dws.tnt.match;

import java.io.File;

import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.matching.MatchingEngine;
import de.uni_mannheim.informatik.dws.winter.utils.Executable;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public abstract class TnTTask extends Executable {
	
	protected MatchingEngine<MatchableTableRow, MatchableTableColumn> matchingEngine;
	public void setMatchingEngine(
			MatchingEngine<MatchableTableRow, MatchableTableColumn> matchingEngine) {
		this.matchingEngine = matchingEngine;
	}
	
	private File outputDirectory;
	/**
	 * @return the outputDirectory
	 */
	public File getOutputDirectory() {
		return outputDirectory;
	}
	/**
	 * @param outputDirectory the outputDirectory to set
	 */
	protected void setOutputDirectory(File outputDirectory) {
		this.outputDirectory = outputDirectory;
	}
	
	public abstract void initialise() throws Exception;
	public abstract void match() throws Exception;
	
	protected static void hello() {
		System.out.println("		 __      __.___        __                     ");
		System.out.println("		 /  \\    /  \\   | _____/  |_  ____     _______ ");
		System.out.println("		 \\   \\/\\/   /   |/    \\   __\\/ __ \\    \\_  __ \\");
		System.out.println("		  \\        /|   |   |  \\  | \\  ___/     |  | \\/");
		System.out.println("		   \\__/\\  / |___|___|  /__|  \\___  > /\\ |__|   ");
		System.out.println("		        \\/           \\/          \\/  \\/        ");
	}
	
	protected void printHeadline(String text) {
//		System.out.println("**********************************************************");
//		System.out.println("** " + text);
//		System.out.println("**********************************************************");
		System.err.println("**********************************************************");
		System.err.println("** " + text);
		System.err.println("**********************************************************");
	}
}
