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
package de.uni_mannheim.informatik.dws.t2k.webtables.parsers;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.zip.GZIPInputStream;

import org.apache.commons.lang.StringUtils;

import au.com.bytecode.opencsv.CSVReader;
import de.uni_mannheim.informatik.dws.t2k.datatypes.DataType;
import de.uni_mannheim.informatik.dws.t2k.normalisation.StringNormalizer;
import de.uni_mannheim.informatik.dws.t2k.webtables.Table;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableMapping;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableRow;

/**
 * Loads a Web Table in the CSV format.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class CsvTableParser {

    private boolean cleanHeader = true;
    public boolean isCleanHeader() {
        return cleanHeader;
    }
    public void setCleanHeader(boolean cleanHeader) {
        this.cleanHeader = cleanHeader;
    }
	
    public Table parseTable(File file) {
        Reader r = null;
        Table t = null;
        try {
            if (file.getName().endsWith(".gz")) {
                GZIPInputStream gzip = new GZIPInputStream(new FileInputStream(file));
                r = new InputStreamReader(gzip, "UTF-8");
            } else {
                r = new InputStreamReader(new FileInputStream(file), "UTF-8");
            }
            
            t = parseTable(r, file.getName());
            
            r.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        
        return t;
    }
    
    public Table parseTable(Reader reader, String fileName) throws IOException {        
		// create new table
	    Table table = new Table();
	    table.setPath(fileName);

	    TableMapping tm = new TableMapping();
	    boolean typesAlreadyDetected = false;
	    
	    try {
	        // create reader
	    	CSVReader csvReader = new CSVReader(reader);
	        
	        // read headers
	        String[] columnNames = csvReader.readNext();
	        
	        if (columnNames == null) {
	            csvReader.close();
	            return null;
	        }
	        
	        // skip annotations
	        // if the current line starts with #, check for valid annotations
	        boolean isMetaData = columnNames[0].startsWith("#");

	        while(isMetaData) {
	            isMetaData = false;
	            
	            // check all valid annotations
	            for(String s : TableMapping.VALID_ANNOTATIONS) {
	                if(columnNames[0].startsWith(s)) {
	                    isMetaData = true;
	                    break;
	                }
	            }
	            
	            // if the current line is an annotation, read the next line and start over
	            if(isMetaData) {
	            	// join the values back together and let the metadata parser handle the line
	            	tm.parseMetadata(StringUtils.join(columnNames, ","));
	            	
	                columnNames = csvReader.readNext();
	                isMetaData = columnNames[0].startsWith("#");
	            }
	        }
	
	        //set the header for each column (take the first row!)
	        //TODO Header detection?
	        int colIdx = 0;
	        for (String columnName : columnNames) {
	            TableColumn c = new TableColumn(colIdx, table);
	
	            // set the header
	            String header = columnName;
	            if (cleanHeader) {
	            	header = StringNormalizer.normaliseHeader(header);
	            }
	            c.setHeader(header);
	
	            if(tm.getDataType(colIdx)!=null) {
	            	c.setDataType((DataType)tm.getDataType(colIdx));
	            	typesAlreadyDetected = true;
	            } else {
	            	c.setDataType(DataType.unknown);
	            }
	            
	            table.addColumn(c);
	            
	            colIdx++;
	        }
	        
	        //int row = 1; // one header row!
	        int row = 0;
            String[] values;

            while((values = csvReader.readNext()) != null) {
            	// make sure the value array has the correct size
            	Object[] rowValues = new Object[table.getSchema().getSize()];
            	for(int i = 0; i < table.getSchema().getSize(); i++) {
            		if(i < values.length) {
            			String value = StringNormalizer.normaliseValue(values[i], false);
            			
            			if(value.equalsIgnoreCase(StringNormalizer.nullValue)) {
            				rowValues[i] = null;
            			} else {
            				rowValues[i] = value;
            			}
            		}
            	}
            	
            	TableRow r = new TableRow(row++, table);
            	r.set(rowValues);
                table.addRow(r);
            }
           
            csvReader.close();
	    } catch(Exception ex) {
	    	ex.printStackTrace();
	    }
	    
	    if(typesAlreadyDetected) {
	    	table.convertValues();
	    } else {
	    	table.inferSchemaAndConvertValues();
	    }
	    
	    if(!table.hasKey()) {
	    	table.identifyKey();
	    }
	    
	    return table;
    }
}
