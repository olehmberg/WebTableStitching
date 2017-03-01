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
package de.uni_mannheim.informatik.dws.t2k.index.webtables;

import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.IntField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.document.Field.Store;


/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class WebTableIndexEntry {

	private String table;
	private String value;
	private String header;
	private Integer column;
	private Integer row;
	
	public static final String TABLE_FIELD = "table";
	public static final String VALUE_FIELD = "value";
	public static final String HEADER_FIELD = "header";
	public static final String COLUMN_FIELD = "column";
	public static final String ROW_FIELD = "row";
	
	public static WebTableIndexEntry fromDocument(Document doc)
	{
		WebTableIndexEntry e = new WebTableIndexEntry();
		
		e.setTable(doc.getField(TABLE_FIELD).stringValue());
		e.setValue(doc.getField(VALUE_FIELD).stringValue());
		e.setValue(doc.getField(HEADER_FIELD).stringValue());
		e.setColumn(doc.getField(COLUMN_FIELD).numericValue().intValue());
		e.setRow(doc.getField(ROW_FIELD).numericValue().intValue());

		return e;
	}
	
	public Document createDocument()
	{
		Document doc = new Document();
		
		doc.add(new StoredField(TABLE_FIELD, table));
		doc.add(new TextField(VALUE_FIELD, value, Field.Store.YES));
		doc.add(new StringField(HEADER_FIELD, header, Store.YES));
		doc.add(new IntField(COLUMN_FIELD, column, Field.Store.YES));
		doc.add(new IntField(ROW_FIELD, row, Field.Store.YES));
		
		return doc;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public Integer getColumn() {
		return column;
	}

	public void setColumn(Integer column) {
		this.column = column;
	}

	public Integer getRow() {
		return row;
	}

	public void setRow(Integer row) {
		this.row = row;
	}

	/**
	 * @return the header
	 */
	public String getHeader() {
		return header;
	}
	
	/**
	 * @param header the header to set
	 */
	public void setHeader(String header) {
		this.header = header;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if(obj!=null && obj instanceof WebTableIndexEntry) {
			WebTableIndexEntry e = (WebTableIndexEntry)obj;
			return table.equals(e.table) && column.equals(e.column) && row.equals(e.row) && value.equals(e.value);
		} else {
			return super.equals(obj);
		}
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		HashCodeBuilder b = new HashCodeBuilder();
		b.append(table);
		b.append(header);
		b.append(column);
		b.append(row);
		b.append(value);
		return b.toHashCode();
	}
}
