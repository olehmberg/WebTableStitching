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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Formats a DefaultModel {@link Record} as CSV
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 * 
 */
public class DefaultRecordCSVFormatter extends CSVFormatter<DefaultRecord, DefaultSchemaElement> {

	@Override
	public String[] getHeader(DataSet<DefaultRecord, DefaultSchemaElement> dataset) {
		List<String> names = new ArrayList<>();
		
		for(DefaultSchemaElement elem : dataset.getAttributes()) {
			names.add(elem.toString());
		}
		
		Collections.sort(names);

		return names.toArray(new String[names.size()]);
	}

	@Override
	public String[] format(DefaultRecord record, DataSet<DefaultRecord, DefaultSchemaElement> dataset) {
		List<String> values = new ArrayList<>(dataset.getAttributes().size());

		List<DefaultSchemaElement> names = new ArrayList<>();
		
		for(DefaultSchemaElement elem : dataset.getAttributes()) {
			names.add(elem);
		}
		Collections.sort(names, new Comparator<DefaultSchemaElement>() {

			@Override
			public int compare(DefaultSchemaElement o1, DefaultSchemaElement o2) {
				return o1.toString().compareTo(o2.toString());
			}
		});

		for (DefaultSchemaElement name : names) {
			values.add(record.getValue(name));
		}

		return values.toArray(new String[values.size()]);
	}

}
