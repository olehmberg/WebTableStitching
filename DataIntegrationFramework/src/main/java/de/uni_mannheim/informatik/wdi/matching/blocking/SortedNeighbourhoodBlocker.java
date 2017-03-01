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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

import de.uni_mannheim.informatik.wdi.matching.MatchingTask;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.DataSet;
import de.uni_mannheim.informatik.wdi.model.Matchable;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.Record;
import de.uni_mannheim.informatik.wdi.model.ResultSet;

/**
 * Implementation of the Sorted-Neighbourhood {@link Blocker}, which based on
 * the blocking key of the {@link BlockingKeyGenerator} compares only the
 * surrounding {@link Record}s.
 * 
 * @author Robert Meusel (robert@dwslab.de)
 * @author Oliver Lehmberg (oli@dwslab.de)
 * 
 * @param <RecordType>
 */
public class SortedNeighbourhoodBlocker<RecordType extends Matchable, SchemaElementType extends Matchable> extends
		DatasetLevelBlocker<RecordType, SchemaElementType> {

	private BlockingKeyGenerator<RecordType> blockingFunction;
	private int windowSize;

	public SortedNeighbourhoodBlocker(
			BlockingKeyGenerator<RecordType> blockingFunction, int windowSize) {
		this.blockingFunction = blockingFunction;
		this.windowSize = windowSize;
	}

	@Override
	public ResultSet<BlockedMatchable<RecordType, SchemaElementType>> block(
			DataSet<RecordType, SchemaElementType> dataset, boolean isSymmetric, ResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences) {
		ResultSet<BlockedMatchable<RecordType, SchemaElementType>> result = new ResultSet<>();

		// add all instances to one list, and compute the keys
		ArrayList<Pair<String, RecordType>> keyIdentifierList = new ArrayList<Pair<String, RecordType>>();
		for (RecordType record : dataset.getRecords()) {
			keyIdentifierList.add(new Pair<String, RecordType>(blockingFunction
					.getBlockingKey(record), record));
		}
		// sort the list by the keys
		Comparator<Pair<String, RecordType>> pairComparator = new Comparator<Pair<String, RecordType>>() {

			@Override
			public int compare(Pair<String, RecordType> o1,
					Pair<String, RecordType> o2) {
				return o1.getFirst().compareTo(o2.getFirst());
			}

		};
		Collections.sort(keyIdentifierList, pairComparator);
		if (isSymmetric) {
			for (int i = 0; i < keyIdentifierList.size() - 1; i++) {
				for (int j = i + 1; ((j - i) < windowSize)
						&& (j < keyIdentifierList.size()); j++) {
					result.add(new MatchingTask<RecordType, SchemaElementType>(keyIdentifierList.get(i).getSecond(),
							keyIdentifierList.get(j).getSecond(), schemaCorrespondences));
				}
			}
		} else {
			for (int i = 0; i < keyIdentifierList.size() - 1; i++) {
				for (int j = Math.max(0, i - windowSize + 1); ((j - i) < windowSize)
						&& (j < keyIdentifierList.size()); j++) {
					result.add(new MatchingTask<RecordType, SchemaElementType>(keyIdentifierList.get(i).getSecond(),
							keyIdentifierList.get(j).getSecond(), schemaCorrespondences));
				}
			}
		}

		calculatePerformance(dataset, dataset, result);
		return result;
	}

	@Override
	public ResultSet<BlockedMatchable<RecordType, SchemaElementType>> block(
			DataSet<RecordType, SchemaElementType> dataset1, DataSet<RecordType, SchemaElementType> dataset2, ResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences) {
		ResultSet<BlockedMatchable<RecordType, SchemaElementType>> result = new ResultSet<>();

		// add all instances to one list, and compute the keys
		ArrayList<Pair<String, RecordType>> keyIdentifierList = new ArrayList<Pair<String, RecordType>>();
		for (RecordType record : dataset1.getRecords()) {
			keyIdentifierList.add(new Pair<String, RecordType>(blockingFunction
					.getBlockingKey(record), record));
		}

		for (RecordType record : dataset2.getRecords()) {
			keyIdentifierList.add(new Pair<String, RecordType>(blockingFunction
					.getBlockingKey(record), record));
		}
		// sort the list by the keys
		Comparator<Pair<String, RecordType>> pairComparator = new Comparator<Pair<String, RecordType>>() {

			@Override
			public int compare(Pair<String, RecordType> o1,
					Pair<String, RecordType> o2) {
				return o1.getFirst().compareTo(o2.getFirst());
			}

		};
		Collections.sort(keyIdentifierList, pairComparator);

		for (int i = 0; i < keyIdentifierList.size() - 1; i++) {
			RecordType r1 = keyIdentifierList.get(i).getSecond();
			
			// make sure r1 belongs to dataset1
			if(dataset1.getRecord(r1.getIdentifier())!=null) {
			
				int counter = 1;
				int j = i;
				while ((counter < windowSize)
						&& (j < (keyIdentifierList.size() - 1))) {
					RecordType r2 = keyIdentifierList.get(++j).getSecond();
					// check if they belong *not* to the same dataset
					if (!r2.getProvenance().equals(r1.getProvenance())) {
						result.add(new MatchingTask<RecordType, SchemaElementType>(r1, r2, schemaCorrespondences));
						counter++;
					}
				}
			
			}
		}

		calculatePerformance(dataset1, dataset2, result);
		return result;
	}
}
