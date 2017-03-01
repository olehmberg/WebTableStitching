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

import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

/**
 * The abstract factory for creating Matchable records from an XML node
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 * 
 * @param <RecordType>
 */
public abstract class MatchableFactory<RecordType extends Matchable> {

	/**
	 * creates a RecordType record from an XML node
	 * 
	 * @param node
	 *            the XML node containing the data
	 * @return
	 */
	public abstract RecordType createModelFromElement(Node node,
			String provenanceInfo);

	/**
	 * returns a value from a child node of the first parameter. The child not
	 * must only have one value (lists will be ignored)
	 * 
	 * @param node
	 *            the node containing the data
	 * @param childName
	 *            the name of the child node
	 * @return
	 */
	protected String getValueFromChildElement(Node node, String childName) {

		// get all child nodes
		NodeList children = node.getChildNodes();

		// iterate over the child nodes until the node with childName is found
		for (int j = 0; j < children.getLength(); j++) {
			Node child = children.item(j);

			// check the node type and the name
			if (child.getNodeType() == org.w3c.dom.Node.ELEMENT_NODE
					&& child.getNodeName().equals(childName)) {

				return child.getTextContent().trim();

			}
		}

		return null;
	}

	/**
	 * returns a list of values from a child node of the first parameter. The
	 * list values are expected to be atomic, i.e. no complex node structures
	 * 
	 * @param node
	 *            the node containing the data
	 * @param childName
	 *            the name of the child node
	 * @return
	 */
	protected List<String> getListFromChildElement(Node node, String childName) {

		// get all child nodes
		NodeList children = node.getChildNodes();

		// iterate over the child nodes until the node with childName is found
		for (int j = 0; j < children.getLength(); j++) {
			Node child = children.item(j);

			// check the node type and name
			if (child.getNodeType() == org.w3c.dom.Node.ELEMENT_NODE
					&& child.getNodeName().equals(childName)) {

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

				return values;
			}
		}

		return null;
	}

	/**
	 * returns a list of records from a child node of the first parameter. The
	 * list values are converted into records by the factory passed as third
	 * parameter.
	 * 
	 * @param node
	 *            the node containing the data
	 * @param childName
	 *            the name of the child node
	 * @param objectNodeName
	 *            the name of the nodes containing the object data
	 * @param factory
	 *            the factory converting child nodes into records of type
	 *            TValueModel
	 * @return
	 */
	protected <ItemType extends Matchable> List<ItemType> getObjectListFromChildElement(
			Node node, String childName, String objectNodeName,
			MatchableFactory<ItemType> factory, String provenanceInfo) {

		// get all child nodes
		NodeList children = node.getChildNodes();

		// iterate over the child nodes until the node with childName is found
		for (int j = 0; j < children.getLength(); j++) {
			Node child = children.item(j);

			// check the node type and name
			if (child.getNodeType() == org.w3c.dom.Node.ELEMENT_NODE
					&& child.getNodeName().equals(childName)) {

				// prepare a list to hold all values
				List<ItemType> values = new ArrayList<>(child.getChildNodes()
						.getLength());

				// iterate the value nodes
				for (int i = 0; i < child.getChildNodes().getLength(); i++) {
					Node valueNode = child.getChildNodes().item(i);

					// check the node type and name
					if (valueNode.getNodeType() == org.w3c.dom.Node.ELEMENT_NODE
							&& valueNode.getNodeName().equals(objectNodeName)) {
						// add the value
						values.add(factory.createModelFromElement(valueNode,
								provenanceInfo));
					}
				}

				return values;
			}
		}

		return null;
	}

}
