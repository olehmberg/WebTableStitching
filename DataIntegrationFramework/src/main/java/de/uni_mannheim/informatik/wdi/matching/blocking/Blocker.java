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
package de.uni_mannheim.informatik.wdi.matching.blocking;

import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.DataSet;
import de.uni_mannheim.informatik.wdi.model.DefaultDataSet;
import de.uni_mannheim.informatik.wdi.model.Matchable;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;

/**
 * The super class for all blocking strategies. The generation of {@link Pair}s
 * based on the {@link Blocker} can be executed for one {@link DefaultDataSet}, where it is
 * used to determine the candidate pairs for duplicate detection of for two
 * {@link DefaultDataSet}s, where it is used to determine the candidate pairs for identity
 * resolution.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 * @author Robert Meusel (robert@dwslab.de)
 * 
 * @param <RecordType>
 */
public abstract class Blocker<RecordType extends Matchable, SchemaElementType extends Matchable>{

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
	
	public abstract ResultSet<BlockedMatchable<RecordType, SchemaElementType>> runBlocking(
			DataSet<RecordType, SchemaElementType> dataset1, 
			DataSet<RecordType, SchemaElementType> dataset2, 
			ResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences,
			DataProcessingEngine engine);
			
	public abstract ResultSet<BlockedMatchable<RecordType, SchemaElementType>> runBlocking(
			DataSet<RecordType, SchemaElementType> dataset, 
			boolean isSymmetric, 
			ResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences,
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
	protected void calculatePerformance(DataSet<RecordType, SchemaElementType> dataset1,
			DataSet<RecordType, SchemaElementType> dataset2,
			ResultSet<BlockedMatchable<RecordType, SchemaElementType>> blocked) {
		long size1 = (long) dataset1.getSize();
		long size2 = (long) dataset2.getSize();
		long maxPairs = size1 * size2;

//		reductionRatio = (double) maxPairs / (double) blocked.size();
		reductionRatio = 1.0 - ((double)blocked.size() / (double)maxPairs);
	}
}
