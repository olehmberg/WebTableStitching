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

import org.joda.time.DateTime;

/**
 * Wrapper for a value during the data fusion process
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 * 
 * @param <ValueType>
 * @param <RecordType>
 */
public class FusableValue<ValueType, RecordType extends Matchable & Fusable<SchemaElementType>, SchemaElementType> {

	private ValueType value;
	private RecordType record;
	private FusableDataSet<RecordType, SchemaElementType> dataset;

	/**
	 * Creates a fusable value from the actual value, the source record and the
	 * source dataset
	 * 
	 * @param value
	 * @param record
	 * @param dataset
	 */
	public FusableValue(ValueType value, RecordType record,
			FusableDataSet<RecordType, SchemaElementType> dataset) {
		this.value = value;
		this.record = record;
		this.dataset = dataset;
	}

	/**
	 * Returns the value
	 * 
	 * @return
	 */
	public ValueType getValue() {
		return value;
	}

	/**
	 * Returns the record that contains the value
	 * 
	 * @return
	 */
	public RecordType getRecord() {
		return record;
	}

	/**
	 * Returns the dataset that contains the value
	 * 
	 * @return
	 */
	public FusableDataSet<RecordType, SchemaElementType> getDataset() {
		return dataset;
	}

	/**
	 * Returns the score of the dataset that contains the value
	 * 
	 * @return
	 */
	public double getDataSourceScore() {
		return dataset.getScore();
	}

	/**
	 * Returns the date of the dataset that contains the value
	 * 
	 * @return
	 */
	public DateTime getDateSourceDate() {
		return dataset.getDate();
	}

}
