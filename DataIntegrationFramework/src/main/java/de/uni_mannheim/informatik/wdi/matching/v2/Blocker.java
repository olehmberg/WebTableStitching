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

import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.DataSet;
import de.uni_mannheim.informatik.wdi.model.Matchable;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public abstract class Blocker<RecordTypeA extends Matchable, RecordTypeB extends Matchable> extends MatchingOperation {

	public Blocker(DataProcessingEngine engine) {
		super(engine);
	}
	
	private double reductionRatio = 1.0;

	/**
	 * Returns the reduction ratio of the last blocking operation. Only
	 * available after calculatePerformance(...) has been called.
	 * 
	 * @return the reduction ratio
	 */
	public double getReductionRatio() {
		return reductionRatio;
	}
	
	public abstract ResultSet<Correspondence<RecordTypeA, RecordTypeB>> runBlocking(
			DataSet<RecordTypeA, RecordTypeB> dataset1, 
			DataSet<RecordTypeA, RecordTypeB> dataset2, 
			ResultSet<Correspondence<RecordTypeB, RecordTypeA>> typeBCorrespondences,
			DataProcessingEngine engine);
			
	public abstract ResultSet<Correspondence<RecordTypeA, RecordTypeB>> runBlocking(
			DataSet<RecordTypeA, RecordTypeB> dataset, 
			boolean isSymmetric, 
			ResultSet<Correspondence<RecordTypeB, RecordTypeA>> typeBCorrespondences,
			DataProcessingEngine engine);
	
	/**
	 * Calculates the reduction ratio. Must be called by all sub classes in
	 * generatePairs(...).
	 * 
	 * @param dataset1
	 *            the first data set
	 * @param dataset2
	 *            the second data set
	 * @param blockedPairs
	 *            the list of pairs that resulted from the blocking
	 */
	protected void calculatePerformance(DataSet<RecordTypeA, RecordTypeB> dataset1,
			DataSet<RecordTypeA, RecordTypeB> dataset2,
			ResultSet<Correspondence<RecordTypeA, RecordTypeB>> blocked) {
		long maxPairs = (long) dataset1.getSize() * (long) dataset2.getSize();

		reductionRatio = 1.0 - ((double)blocked.size() / (double)maxPairs);
	}
}
