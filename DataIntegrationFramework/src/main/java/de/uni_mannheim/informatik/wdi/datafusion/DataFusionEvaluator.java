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
package de.uni_mannheim.informatik.wdi.datafusion;

import java.util.HashMap;

import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.DataSet;
import de.uni_mannheim.informatik.wdi.model.Fusable;
import de.uni_mannheim.informatik.wdi.model.FusableDataSet;
import de.uni_mannheim.informatik.wdi.model.Matchable;
import de.uni_mannheim.informatik.wdi.model.RecordGroup;
import de.uni_mannheim.informatik.wdi.model.RecordGroupFactory;
import de.uni_mannheim.informatik.wdi.model.ResultSet;

/**
 * Evaluates a data fusion result based on a given {@link DataFusionStrategy}
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 * 
 * @param <RecordType>
 */
public class DataFusionEvaluator<RecordType extends Matchable & Fusable<SchemaElementType>, SchemaElementType extends Matchable> {

	private DataFusionStrategy<RecordType, SchemaElementType> strategy;
	private RecordGroupFactory<RecordType, SchemaElementType> groupFactory;
	
	private boolean verbose = false;

	/**
	 * Returns whether additional information will be written to the console
	 * 
	 * @return
	 */
	public boolean isVerbose() {
		return verbose;
	}

	/**
	 * Sets whether additional information will be written to the console
	 * 
	 * @param verbose
	 */
	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}

	/**
	 * Creates a new instance with the provided strategy
	 * 
	 * @param strategy
	 */
	public DataFusionEvaluator(DataFusionStrategy<RecordType, SchemaElementType> strategy, RecordGroupFactory<RecordType, SchemaElementType> factory) {
		this.strategy = strategy;
		this.groupFactory = factory;
	}

	/**
	 * Evaluates the the data fusion result against a gold standard
	 * 
	 * @param dataset
	 * @param goldStandard
	 * @return the accuracy of the data fusion result
	 */
	public double evaluate(FusableDataSet<RecordType, SchemaElementType> dataset,
			DataSet<RecordType, SchemaElementType> goldStandard, ResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences) {

		int correctValues = 0;
		int totalValues = goldStandard.getSize()
				* strategy.getAttributeFusers(null, schemaCorrespondences).size();
		HashMap<SchemaElementType, Integer> attributeCount = new HashMap<SchemaElementType, Integer>();
		for (AttributeFusionTask<RecordType, SchemaElementType> fusionTask : strategy.getAttributeFusers(null, schemaCorrespondences)) {
			attributeCount.put(fusionTask.getSchemaElement(), 0);
		}

		for (RecordType record : goldStandard.getRecords()) {
			RecordType fused = dataset.getRecord(record.getIdentifier());

			if (fused != null) {
				
				// ask strategy to compare record and fused based on schema correspondences
				RecordGroup<RecordType, SchemaElementType> g = groupFactory.createRecordGroup();
				g.addRecord(record.getIdentifier(), dataset);
				
				for (AttributeFusionTask<RecordType, SchemaElementType> fusionTask : strategy.getAttributeFusers(g, schemaCorrespondences)) {
					EvaluationRule<RecordType, SchemaElementType> r = fusionTask.getEvaluationRule();

					if (r.isEqual(fused, record, fusionTask.getSchemaElement())) { 
						correctValues++;
						attributeCount.put(fusionTask.getSchemaElement(),
								attributeCount.get(fusionTask.getSchemaElement()) + 1);
					} else if (verbose) {
						System.out.println(String.format(
								"[error] %s: %s <> %s", r.getClass()
										.getSimpleName(), fused.toString(),
								record.toString()));
					}
				}
			}
		}
		if (verbose) {
			System.out.println("Attribute-specific Accuracy:");
			for (AttributeFusionTask<RecordType, SchemaElementType> fusionTask : strategy.getAttributeFusers(null, schemaCorrespondences)) {
				double acc = (double) attributeCount.get(fusionTask.getSchemaElement())
						/ (double) goldStandard.getSize();
				System.out.println(String.format("	%s: %.2f", fusionTask.getSchemaElement().getIdentifier(), acc));

			}
		}

		return (double) correctValues / (double) totalValues;
	}
}
