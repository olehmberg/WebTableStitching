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
package de.uni_mannheim.informatik.dws.t2k.webtables;

import java.io.Serializable;

/**
 * Contains information about the context of a Web Table.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class TableContext implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	private String url;
	private String pageTitle;
	private String tableTitle;
	private int tableNum;
	private String textBeforeTable;
	private String textAfterTable;
	private String timestampBeforeTable;
	private String timestampAfterTable;
	private String lastModified;
	
	/**
	 * Returns the URL from which this table was extracted
	 * @return
	 */
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	
	/**
	 * Returns the title (<meta><title>) of the HTML page from which this table was extracted
	 * @return
	 */
	public String getPageTitle() {
		return pageTitle;
	}
	public void setPageTitle(String pageTitle) {
		this.pageTitle = pageTitle;
	}
	
	/**
	 * Returns the text of the closest heading (<h...>) to the table
	 * @return
	 */
	public String getTableTitle() {
		return tableTitle;
	}
	public void setTableTitle(String tableTitle) {
		this.tableTitle = tableTitle;
	}
	
	/**
	 * Returns the table's index on the HTML page (i.e., 0 means it's the first table, 1 means it's the second table, etc.)
	 * @return
	 */
	public int getTableNum() {
		return tableNum;
	}
	public void setTableNum(int tableNum) {
		this.tableNum = tableNum;
	}
	
	/**
	 * Returns the text before the table
	 * @return
	 */
	public String getTextBeforeTable() {
		return textBeforeTable;
	}
	public void setTextBeforeTable(String textBeforeTable) {
		this.textBeforeTable = textBeforeTable;
	}
	
	/**
	 * Returns the text after the table
	 * @return
	 */
	public String getTextAfterTable() {
		return textAfterTable;
	}
	public void setTextAfterTable(String textAfterTable) {
		this.textAfterTable = textAfterTable;
	}
	
	/**
	 * Returns the text before the table that contains a timestamp
	 * @return
	 */
	public String getTimestampBeforeTable() {
		return timestampBeforeTable;
	}
	public void setTimestampBeforeTable(String timestampBeforeTable) {
		this.timestampBeforeTable = timestampBeforeTable;
	}
	
	/**
	 * Returns the text after the table that contains a timestamp
	 * @return
	 */
	public String getTimestampAfterTable() {
		return timestampAfterTable;
	}
	public void setTimestampAfterTable(String timestampAfterTable) {
		this.timestampAfterTable = timestampAfterTable;
	}
	
	/**
	 * Returns the last modified data of the HTML page
	 * @return
	 */
	public String getLastModified() {
		return lastModified;
	}
	public void setLastModified(String lastModified) {
		this.lastModified = lastModified;
	}
	
	
}
