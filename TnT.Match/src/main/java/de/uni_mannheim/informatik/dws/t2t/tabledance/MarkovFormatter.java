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
package de.uni_mannheim.informatik.dws.t2t.tabledance;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.commons.io.IOUtils;

import de.uni_mannheim.informatik.dws.t2k.utils.StringUtils;
import de.uni_mannheim.informatik.dws.t2k.utils.data.MapUtils;
import de.uni_mannheim.informatik.wdi.clustering.ConnectedComponentClusterer;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.Triple;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class MarkovFormatter {

	private StringBuilder sb = new StringBuilder();
	
	public void addForbiddenLink(String from, String to) {
		sb.append(String.format("!cor(\"%s\",\"%s\")\n", from ,to));
	}
	
	public void addLinkWithConfidence(String from, String to, double confidence) {
		DecimalFormat df = new DecimalFormat("0", DecimalFormatSymbols.getInstance(Locale.ENGLISH));
		df.setMaximumFractionDigits(340); 
		
		sb.append(String.format("corConf(\"%s\", \"%s\", %s)\n", from, to, df.format(confidence)));
	}
	
	public void write(File f) throws IOException {
		FileWriter fw = new FileWriter(f);
		IOUtils.write(sb.toString(), fw);
		fw.close();
	}
	
	public void summariseResult(File result) throws IOException {
		BufferedReader r = new BufferedReader(new FileReader(result));
		
		ConnectedComponentClusterer<Pair<String, Integer>> comp = new ConnectedComponentClusterer<>();
		
		String line = null;
		
		HashMap<String, List<String>> mapping = new HashMap<>();
		
		while((line=r.readLine())!=null) {
			line = line.replace("cor(\"", "").replace("\")", "");
			String[] values = line.split("\", \"");
			
			String[] col1 = values[0].split("//");
			String[] col2 = values[1].split("//");
			
			List<String> lst = MapUtils.get(mapping, col1[0], new LinkedList<String>());
			lst.add(col1[1] + " -> " + values[1]);
			
			lst = MapUtils.get(mapping, col2[0], new LinkedList<String>());
			lst.add(col2[1] + " -> " + values[1]);
			
			comp.addEdge(new Triple<>(new Pair<>(col1[0], Integer.parseInt(col1[1])), new Pair<>(col2[0], Integer.parseInt(col2[1])), 1.0));
		}
		
		r.close();

    	Map<Collection<Pair<String, Integer>>, Pair<String, Integer>> clustering = comp.createResult();
    	int idx = 0;
    	for(Collection<Pair<String, Integer>> clu : clustering.keySet()) {
    		System.out.print("Cluster " + idx++ + ": ");
    		
    		//ArrayList<String> labels = new ArrayList<>();
    		HashSet<String> labels = new HashSet<>();
    		
    		for(Pair<String, Integer> edge : clu) {
    			//System.out.println(String.format("\t%s//%d", edge.getFirst(), edge.getSecond()));
    			labels.add(edge.getFirst().split("\\+")[edge.getSecond()] + "[" + edge.getSecond() + "]");
    		}
    		
    		//Collections.sort(labels);
    		
    		System.out.println("\t" + StringUtils.join(labels, ","));
    	}
		
//		System.out.println("Mapping: ");
//		ArrayList<String> keys = new ArrayList<>(mapping.keySet());
//		Collections.sort(keys);
//		for(String schema : keys) {
//			System.out.println(schema);
//			
//			Collections.sort(mapping.get(schema));
//			
//			for(String cor : mapping.get(schema)) {
//				System.out.println("\t" + cor);
//			}
//		}
	}
	
	public static void main(String[] args) throws IOException {
		MarkovFormatter mf = new MarkovFormatter();
		
		mf.summariseResult(new File(args[0]));
	}
}
