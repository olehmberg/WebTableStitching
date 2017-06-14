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
package de.uni_mannheim.informatik.dws.tnt.match;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;

import de.uni_mannheim.informatik.dws.winter.webtables.Table;
import de.uni_mannheim.informatik.dws.winter.webtables.TableColumn;
import de.uni_mannheim.informatik.dws.winter.webtables.TableRow;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class TableReport {

	public void writeTableReport(Collection<Table> tables, File file) throws IOException {
		
		BufferedWriter w = new BufferedWriter(new FileWriter(file));
		
		for(Table t : tables) {
		
			w.write(String.format("### %s ###\n", t.getPath()));
			
			w.write(String.format("Schema: %d Columns\n", t.getSchema().getSize()));
			
			for(TableColumn c : t.getSchema().getRecords()) {
				int values = 0;
				for(TableRow r : t.getRows()) {
					if(r.get(c.getColumnIndex())!=null) {
						values++;
					}
				}
				double density = (values/(double)t.getRows().size());
				
				w.write(String.format("\t%d\t%s\t(%.4f)\n", c.getColumnIndex(), c.getHeader(), density));
			}
			
//			w.write(String.format("%d Functional Dependencies\n", t.getSchema().getFunctionalDependencies().size()));
//			for(Collection<TableColumn> det : t.getSchema().getFunctionalDependencies().keySet()) {
//				Collection<TableColumn> dep = t.getSchema().getFunctionalDependencies().get(det);
//				w.write(String.format("\t{%s}->{%s}\n", 
//						Q.project(det, new TableColumn.ColumnIndexAndHeaderProjection()),
//						Q.project(dep, new TableColumn.ColumnIndexAndHeaderProjection())));
//			}
//			
//			w.write(String.format("%d Candidate Keys\n", t.getSchema().getCandidateKeys().size()));
//			for(Collection<TableColumn> k : t.getSchema().getCandidateKeys()) {
//				w.write(String.format("\t{%s}\n", Q.project(k, new TableColumn.ColumnIndexAndHeaderProjection())));
//			}
			
//			w.write("Column Density:\n");
//			
//			for(TableColumn col : t.getColumns()) {
//				int values = 0;
//				for(TableRow r : t.getRows()) {
//					if(r.get(col.getColumnIndex())!=null) {
//						values++;
//					}
//				}
//				w.write(String.format("\t%.4f\t%s\n", (values/(double)t.getRows().size()), col));
//			}
		}
		
		w.close();
		
	}
	
}
