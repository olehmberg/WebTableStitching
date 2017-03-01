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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.xpath.XPath;
import javax.xml.xpath.XPathConstants;
import javax.xml.xpath.XPathExpression;
import javax.xml.xpath.XPathExpressionException;
import javax.xml.xpath.XPathFactory;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import au.com.bytecode.opencsv.CSVWriter;

/**
 * A Data set contains a set of {@link Record}.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 * 
 * @param <RecordType>
 */
public class DefaultDataSet<RecordType extends Matchable, SchemaElementType> implements DataSet<RecordType, SchemaElementType> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	/**
	 * HashMap of an identifier and the actual {@link Record}.
	 */
	protected Map<String, RecordType> records;

	public DefaultDataSet() {
		records = new HashMap<>();
	}

	/**
	 * Loads a data set from an XML file
	 * 
	 * @param dataSource
	 *            the XML file containing the data
	 * @param entryFactory
	 *            the Factory that creates the Model instances from the XML
	 *            nodes
	 * @param entriesPath
	 *            the XPath to the XML nodes representing the entries
	 * @throws ParserConfigurationException
	 * @throws IOException
	 * @throws SAXException
	 * @throws XPathExpressionException
	 */
	public void loadFromXML(File dataSource,
			MatchableFactory<RecordType> modelFactory, String recordPath)
			throws ParserConfigurationException, SAXException, IOException,
			XPathExpressionException {
		// create objects for reading the XML file
		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder;
		builder = factory.newDocumentBuilder();
		Document doc = builder.parse(dataSource);

		// prepare the XPath that selects the entries
		XPathFactory xPathFactory = XPathFactory.newInstance();
		XPath xpath = xPathFactory.newXPath();
		XPathExpression expr = xpath.compile(recordPath);

		// execute the XPath to get all entries
		NodeList list = (NodeList) expr.evaluate(doc, XPathConstants.NODESET);

		if (list.getLength() == 0) {
			System.out.println("ERROR: no elements matching the XPath ("
					+ recordPath + ") found in the input file "
					+ dataSource.getAbsolutePath());
		} else {
			System.out.println(String.format("Loading %d elements from %s",
					list.getLength(), dataSource.getName()));

			// create entries from all nodes matching the XPath
			for (int i = 0; i < list.getLength(); i++) {

				// create the entry, use file name as provenance information
				RecordType record = modelFactory.createModelFromElement(
						list.item(i), dataSource.getName());

				if (record != null) {
					// add it to the data set
					addRecord(record);
				} else {
					System.out.println(String.format(
							"Could not generate entry for ", list.item(i)
									.getTextContent()));
				}
			}
		}
	}

	/**
	 * Returns a collection with all entries of this data set.
	 * 
	 * @return
	 */
	@Override
	public Collection<RecordType> getRecords() {
		return records.values();
	}

	/**
	 * Returns the entry with the specified identifier or null, if it is not
	 * found.
	 * 
	 * @param identifier
	 *            The identifier of the entry that should be returned
	 * @return
	 */
	@Override
	public RecordType getRecord(String identifier) {
		return records.get(identifier);
	}

	/**
	 * Returns the number of entries in this data set
	 * 
	 * @return
	 */
	@Override
	public int getSize() {
		return records.size();
	}

	/**
	 * Adds an entry to this data set. Any existing entry with the same
	 * identifier will be replaced.
	 * 
	 * @param entry
	 */
	@Override
	public void addRecord(RecordType record) {
		records.put(record.getIdentifier(), record);
	}

	/**
	 * Returns a random record from the data set
	 * 
	 * @return
	 */
	@Override
	public RecordType getRandomRecord() {
		Random r = new Random();

		List<RecordType> allRecords = new ArrayList<>(records.values());

		int index = r.nextInt(allRecords.size());

		return allRecords.get(index);
	}

	/***
	 * Removes all records from this dataset
	 */
	@Override
	public void ClearRecords() {
		records.clear();
	}
	
	/**
	 * Writes the data set to a CSV file
	 * 
	 * @param file
	 * @param formatter
	 * @throws IOException
	 */
	public void writeCSV(File file, CSVFormatter<RecordType, SchemaElementType> formatter)
			throws IOException {
		CSVWriter writer = new CSVWriter(new FileWriter(file));

		String[] headers = formatter.getHeader(this);
		writer.writeNext(headers);

		for (RecordType record : records.values()) {
			String[] values = formatter.format(record, this);

			writer.writeNext(values);
		}

		writer.close();
	}

	/**
	 * Writes this dataset to an XML file using the specified formatter
	 * 
	 * @param outputFile
	 * @param formatter
	 * @throws ParserConfigurationException
	 * @throws TransformerException
	 * @throws FileNotFoundException
	 */
	public void writeXML(File outputFile, XMLFormatter<RecordType> formatter)
			throws ParserConfigurationException, TransformerException,
			FileNotFoundException {

		DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
		DocumentBuilder builder;

		builder = factory.newDocumentBuilder();
		Document doc = builder.newDocument();
		Element root = formatter.createRootElement(doc);

		doc.appendChild(root);

		for (RecordType record : getRecords()) {
			root.appendChild(formatter.createElementFromRecord(record, doc));
		}

		TransformerFactory tFactory = TransformerFactory.newInstance();
		Transformer transformer;
		transformer = tFactory.newTransformer();
		transformer.setOutputProperty(OutputKeys.INDENT, "yes");
		transformer.setOutputProperty(
				"{http://xml.apache.org/xslt}indent-amount", "2");
		DOMSource source = new DOMSource(root);
		StreamResult result = new StreamResult(new FileOutputStream(outputFile));

		transformer.transform(source, result);

	}
	
	private List<SchemaElementType> attributes = new LinkedList<>();
	
	public void addAttribute(SchemaElementType attribute) {
		attributes.add(attribute);
	}
	
	/**
	 * @return the attributes
	 */
	public List<SchemaElementType> getAttributes() {
		return attributes;
	}

	//TODO refactor such that dataset and basiccollection use the same methods ...
	
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.model.BasicCollection#add(java.lang.Object)
	 */
	@Override
	public void add(RecordType element) {
		addRecord(element);
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.model.BasicCollection#get()
	 */
	@Override
	public Collection<RecordType> get() {
		return getRecords();
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.model.BasicCollection#size()
	 */
	@Override
	public int size() {
		return getSize();
	}
	
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.model.DataSet#removeRecord(java.lang.String)
	 */
	@Override
	public void removeRecord(String identifier) {
		records.remove(identifier);
	}
}
