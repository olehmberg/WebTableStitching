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

import org.apache.log4j.Logger;

import de.uni_mannheim.informatik.dws.t2k.utils.cli.Executable;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.wdi.matching.MatchingEngine;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public abstract class TnTTask extends Executable {

	protected Logger log = Logger.getLogger("TnTLogger");
	/**
	 * @param log the log to set
	 */
	public void setLogger(Logger log) {
		this.log = log;
	}
	
	protected DataProcessingEngine proc;
	public void setDataProcessingEngine(DataProcessingEngine engine) {
		proc = engine;
	}
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
		System.out.println("                     __,-~~/~    `---.");
		System.out.println("                   _/_,---(      ,    )");
		System.out.println("               __ /        <    /   )  \\___");
		System.out
				.println("- ------===;;;'====------------------===;;;===----- -  -");
		System.out
				.println("                  \\/  ~\"~\"~\"~\"~\"~\\~\"~)~\"/\")");
		System.out.println("                  (_ (   \\  (     >    \\)");
		System.out.println("                   \\_( _ <         >_>'");
		System.out.println("                      ~ `-i' ::>|--\"");
		System.out.println("                          I;|.|.|");
		System.out.println("                         <|i::|i|`.");
		System.out.println("                        (` ^'\"`-' \")");
		System.out.println("                 T H E   D A T O N A T O R");
	}
	
	protected void printHeadline(String text) {
		System.out.println("**********************************************************");
		System.out.println("** " + text);
		System.out.println("**********************************************************");
		System.err.println("**********************************************************");
		System.err.println("** " + text);
		System.err.println("**********************************************************");
	}
}
