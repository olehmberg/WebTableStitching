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
import java.util.List;
import java.util.Map;

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * Factory class for creating DefaultModel records from an XML node. All child
 * nodes on the first level are added as attributes.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 * 
 */
public class DefaultRecordFactory extends MatchableFactory<DefaultRecord> {

	private String idAttributeName;
	private Map<String, DefaultSchemaElement> attributeMapping;
	
	public DefaultRecordFactory(String idAttributeName, Map<String, DefaultSchemaElement> attributeMapping) {
		this.idAttributeName = idAttributeName;
		this.attributeMapping = attributeMapping;
	}

	@Override
	public DefaultRecord createModelFromElement(Node node, String provenanceInfo) {
		String id = getValueFromChildElement(node, idAttributeName);

		DefaultRecord model = new DefaultRecord(id, provenanceInfo);

		// get all child nodes
		NodeList children = node.getChildNodes();

		// iterate over the child nodes until the node with childName is found
		for (int j = 0; j < children.getLength(); j++) {
			Node child = children.item(j);

			// check the node type
			if (child.getNodeType() == org.w3c.dom.Node.ELEMENT_NODE
					&& child.getChildNodes().getLength() > 0) {

				// single value or list?
				if (child.getChildNodes().getLength() == 1) {

					// single value
					DefaultSchemaElement att = attributeMapping.get(child.getNodeName());
					model.setValue(att, child.getTextContent()
							.trim());

				} else {

					// list
					// prepare a list to hold all values
					List<String> values = new ArrayList<>(child.getChildNodes()
							.getLength());

					// iterate the value nodes
					for (int i = 0; i < child.getChildNodes().getLength(); i++) {
						Node valueNode = child.getChildNodes().item(i);
						String value = valueNode.getTextContent().trim();

						// add the value
						values.add(value);
					}

					DefaultSchemaElement att = attributeMapping.get(child.getNodeName());
					model.setList(att, values);

				}

			}

		}

		return model;
	}

}
