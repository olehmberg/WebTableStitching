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
package de.uni_mannheim.informatik.wdi.model;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

/**
 * Represents a group of {@link Record}s
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 * 
 * @param <RecordType>
 */
public class RecordGroup<RecordType extends Matchable & Fusable<SchemaElementType>, SchemaElementType> {

	private Map<String, FusableDataSet<RecordType, SchemaElementType>> records;

	protected RecordGroup() {
		records = new HashMap<>();
	}

	/**
	 * Adds a record to this group
	 * 
	 * @param id
	 * @param dataset
	 */
	public void addRecord(String id, FusableDataSet<RecordType, SchemaElementType> dataset) {
		records.put(id, dataset);
	}

	/**
	 * Adds all records from the provided group to this group
	 * 
	 * @param otherCluster
	 */
	public void mergeWith(RecordGroup<RecordType, SchemaElementType> otherGroup) {
		records.putAll(otherGroup.records);
	}

	/**
	 * Returns the IDs of all records in this group
	 * 
	 * @return
	 */
	public Set<String> getRecordIds() {
		return records.keySet();
	}

	/**
	 * Returns the size of this group
	 * 
	 * @return
	 */
	public int getSize() {
		return records.size();
	}

	/**
	 * Returns all records of this group
	 * 
	 * @return
	 */
	public Collection<RecordType> getRecords() {
		Collection<RecordType> result = new LinkedList<>();

		for (String id : records.keySet()) {
			DataSet<RecordType, SchemaElementType> ds = records.get(id);

			result.add(ds.getRecord(id));
		}

		return result;
	}

	/**
	 * Returns all records of this group with their corresponding datasets
	 * 
	 * @return
	 */
	public Collection<Pair<RecordType, FusableDataSet<RecordType, SchemaElementType>>> getRecordsWithDataSets() {
		Collection<Pair<RecordType, FusableDataSet<RecordType, SchemaElementType>>> result = new LinkedList<>();

		for (String id : records.keySet()) {
			FusableDataSet<RecordType, SchemaElementType> ds = records.get(id);
			RecordType record = ds.getRecord(id);
			result.add(new Pair<>(record, ds));
		}

		return result;
	}
	
	/**
	 * Returns the schema correspondence that connects the given record with the target schema
	 * 
	 * @param record
	 * @param schemaCorrespondences
	 * @return
	 */
	public Correspondence<SchemaElementType, RecordType> getSchemaCorrespondenceForRecord(RecordType record, ResultSet<Correspondence<SchemaElementType, RecordType>> schemaCorrespondences, SchemaElementType schemaElement) {
		// as the is currently no connection between RecordType and SchemaElementType in the model, this has to be implemented by the application using schema correspondences
		return null;
	}
}
