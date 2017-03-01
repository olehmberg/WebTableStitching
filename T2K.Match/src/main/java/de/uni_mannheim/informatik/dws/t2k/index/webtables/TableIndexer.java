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

import java.io.IOException;

import org.apache.lucene.index.IndexWriter;

import de.uni_mannheim.informatik.dws.t2k.datatypes.DataType;
import de.uni_mannheim.informatik.dws.t2k.index.IIndex;
import de.uni_mannheim.informatik.dws.t2k.webtables.Table;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableRow;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class TableIndexer {

	   public void indexStringsExact(IIndex index, Table t)
	    {
	        IndexWriter writer = index.getIndexWriter();
	        
	        long cnt=0;
	        
	        for(TableRow row : t.getRows()) {
	        	
	        	for(TableColumn col : t.getColumns()) {
	        	
	        		if(col.getDataType()==DataType.string) {
	        			
	        			if(row.get(col.getColumnIndex())!=null) {
	        			
		        			WebTableIndexEntry e = new WebTableIndexEntry();
		        			
		        			e.setTable(t.getPath());
		        			e.setColumn(col.getColumnIndex());
		        			e.setRow(row.getRowNumber());
		        			e.setHeader(col.getHeader());
		        			e.setValue(row.get(col.getColumnIndex()).toString());

				            try {
				                writer.addDocument(e.createDocument());
				            } catch (IOException e1) {
				                e1.printStackTrace();
				            }
				            
				            cnt++;
				            
				            if(cnt%100000==0)
				            {
				                System.out.println("Indexed " + cnt + " items.");
				            }
	            
	        			}
			            
	        		}
	        	}
	        }
	        
	        System.out.println("Indexed " + cnt + " items.");
	        
	        index.closeIndexWriter();
	    }
	
}
