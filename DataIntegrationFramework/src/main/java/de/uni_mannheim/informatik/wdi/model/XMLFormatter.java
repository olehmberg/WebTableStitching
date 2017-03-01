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

import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 * Abstract super class for specifying how a {@link DefaultDataSet} should be
 * transformed into XML
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 * 
 * @param <RecordType>
 */
public abstract class XMLFormatter<RecordType> {

	/**
	 * Creates the root element for a collection of records
	 * 
	 * @param doc
	 * @return
	 */
	public abstract Element createRootElement(Document doc);

	/**
	 * Creates an element representing a record
	 * 
	 * @param record
	 * @param doc
	 * @return
	 */
	public abstract Element createElementFromRecord(RecordType record,
			Document doc);

	/**
	 * Creates a text element with the specified element name and the value as
	 * content
	 * 
	 * @param name
	 * @param value
	 * @param doc
	 * @return
	 */
	protected Element createTextElement(String name, String value, Document doc) {
		Element elem = doc.createElement(name);
		if (value != null) {
			elem.appendChild(doc.createTextNode(value));
		}
		return elem;
	}
}
