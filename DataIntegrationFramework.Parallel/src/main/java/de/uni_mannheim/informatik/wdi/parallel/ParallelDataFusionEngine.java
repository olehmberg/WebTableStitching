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
package de.uni_mannheim.informatik.wdi.parallel;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import de.uni_mannheim.informatik.dws.t2k.utils.concurrent.Consumer;
import de.uni_mannheim.informatik.dws.t2k.utils.concurrent.Parallel;
import de.uni_mannheim.informatik.wdi.datafusion.CorrespondenceSet;
import de.uni_mannheim.informatik.wdi.datafusion.DataFusionEngine;
import de.uni_mannheim.informatik.wdi.datafusion.DataFusionStrategy;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.Fusable;
import de.uni_mannheim.informatik.wdi.model.FusableDataSet;
import de.uni_mannheim.informatik.wdi.model.Matchable;
import de.uni_mannheim.informatik.wdi.model.RecordGroup;
import de.uni_mannheim.informatik.wdi.model.ResultSet;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class ParallelDataFusionEngine<RecordType extends Matchable & Fusable<SchemaElementType>, SchemaElementType extends Matchable> extends DataFusionEngine<RecordType, SchemaElementType> {

	/**
	 * @param strategy
	 */
	public ParallelDataFusionEngine(
			DataFusionStrategy<RecordType, SchemaElementType> strategy) {
		super(strategy);
		// TODO Auto-generated constructor stub
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.datafusion.DataFusionEngine#run(de.uni_mannheim.informatik.wdi.datafusion.CorrespondenceSet, de.uni_mannheim.informatik.wdi.model.ResultSet)
	 */
	@Override
	public FusableDataSet<RecordType, SchemaElementType> run(
			CorrespondenceSet<RecordType, SchemaElementType> correspondences,
			ResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences) {
		FusableDataSet<RecordType, SchemaElementType> fusedDataSet = new FusableDataSet<>();

		for (RecordGroup<RecordType, SchemaElementType> clu : correspondences.getRecordGroups()) {
			RecordType fusedRecord = getStrategy().apply(clu, schemaCorrespondences);
			fusedDataSet.addRecord(fusedRecord);

			for (RecordType record : clu.getRecords()) {
				fusedDataSet.addOriginalId(fusedRecord, record.getIdentifier());
			}
		}

		return fusedDataSet;
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.datafusion.DataFusionEngine#getAttributeConsistencies(de.uni_mannheim.informatik.wdi.datafusion.CorrespondenceSet, de.uni_mannheim.informatik.wdi.model.ResultSet)
	 */
	@Override
	public Map<String, Double> getAttributeConsistencies(
			CorrespondenceSet<RecordType, SchemaElementType> correspondences,
			final ResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences) {
		final ConcurrentHashMap<String, Double> consistencySums = new ConcurrentHashMap<>();
		final ConcurrentHashMap<String, Integer> consistencyCounts = new ConcurrentHashMap<>();
		
		// changed to calculation as follows:
		// degree of consistency per instance = percentage of most frequent value
		// consistency = average of degree of consistency per instance
		
		// for each record group (=instance in the target dataset), calculate the degree of consistency for each attribute
		
		System.out.println(String.format("Calculating consistency for %d record groups", correspondences.getRecordGroups().size()));
		
		new Parallel<RecordGroup<RecordType, SchemaElementType>>().tryForeach(correspondences.getRecordGroups(), new Consumer<RecordGroup<RecordType, SchemaElementType>>() {

			@Override
			public void execute(
					RecordGroup<RecordType, SchemaElementType> clu) {
				try {
					Map<String, Double> values = getStrategy()
							.getAttributeConsistency(clu, schemaCorrespondences);
	
					synchronized (consistencyCounts) {
						for (String att : values.keySet()) {
							Double consistencyValue = values.get(att);
							
							if(consistencyValue!=null) {
								Integer cnt = consistencyCounts.get(att);
								if (cnt == null) {
									cnt = 0;
								}
								consistencyCounts.put(att, cnt + 1);
								
								Double sum = consistencySums.get(att);
								if(sum == null) {
									sum = 0.0;
								}
								consistencySums.put(att, sum + consistencyValue);
							}
						}
					}
				
				} catch(Exception e) {
					e.printStackTrace();
				}
			}
		});

		Map<String, Double> result = new HashMap<>();
		for (String att : consistencySums.keySet()) {
			if(consistencySums.get(att)!=null) {
				// divide by count, not total number of record groups as we only consider groups that actually have a value
				double consistency = consistencySums.get(att)
						/ (double) consistencyCounts.get(att);
				
				if(consistency>1.0) {
					System.out.println("Wrong Consistency!");
				}
				
				result.put(att, consistency);
			}
		}

		return result;
	}
}
