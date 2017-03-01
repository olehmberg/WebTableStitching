package de.uni_mannheim.informatik.dws.t2k.match.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import de.uni_mannheim.informatik.dws.t2k.normalisation.StringNormalizer;

public class SurfaceForms implements Serializable{
	
	//TODO (Done) Sani: combine surface forms and redirects. All additional labels will be used on the DBpedia side (not for the Web Tables). Add additional labels to all string columns.
	
	/**
	 * loads surface forms in the form of HashMap<original_entity, <set of related surface forms>>, for example HashMap<abc, <cba, ABC, CBA>>
	 */
	private static final long serialVersionUID = 1L;
	
	private File locationSF, locationRD;
	private static int rdfsLabel = 1;
	
	public void setRdfsLabel(int rdfsLabel) {
		SurfaceForms.rdfsLabel = rdfsLabel;
	}

	//	entity and its surface forms 
	private static transient HashMap<String, HashSet<String>> surfaceForms = null;
	
	public SurfaceForms(File locationSF, File locationRD) {
		this.locationSF = locationSF;
		this.locationRD = locationRD;
		
		if(locationSF==null && locationRD==null) {
			surfaceForms = new HashMap<String, HashSet<String>>();
		}
	}
	
	public SurfaceForms(HashMap<String, HashSet<String>> surfaceForms){
		SurfaceForms.surfaceForms = surfaceForms;
	}

//	return surface forms (HashSet) if available
	public Set<String> getSurfaceForms(String forLabel) {
		if(surfaceForms==null || surfaceForms.containsKey(forLabel)) {
			return surfaceForms.get(forLabel);
		} else {
			return new HashSet<>();
		}
	}

	protected void setSurfaceForms(HashMap<String, HashSet<String>> surfaceForms) {
		SurfaceForms.surfaceForms = surfaceForms;
	}
	
//	method to load surface forms and redirects when ever needed, 
//	makes use of thread locking mechanism to ensure that only one thread at a time get access to the instance (to prevent creating multiple instances) 
	public void loadIfRequired() {
		if(surfaceForms==null) {
			synchronized (this) {
				if(surfaceForms==null) {
					loadSurfaceForms();
				}
			}
		}
	}
	
	
//	method to load surface forms and redirects based on given location
	public void loadSurfaceForms(){
		
//		surface forms and redirects will be kept in same map as they both are used for the same purpose in the project.
		HashMap<String, HashSet<String>> surfaceForms = new HashMap<String, HashSet<String>>(); // this would be correct
//		surfaceForms = new HashMap<String, HashSet<String>>(); // this is not correct
		
		BufferedReader br = null;
		
//		load surface forms if location is available
		if(locationSF!=null)
		{	
			try {
				System.out.println("Loading Surface Forms...");
				
				String sCurrentLine;
	
				br = new BufferedReader(new FileReader(locationSF));
	
				while ((sCurrentLine = br.readLine()) != null) {
					HashSet<String> set = new HashSet<String>();
					
//					because the file is 'Tab Delimited', we split the every line with 'tab' to separate entity from its surface forms.
					String[] tabDelimitedLine = sCurrentLine.split("\\t");
			        
					for(int i=1; i<tabDelimitedLine.length; i++){
			        		set.add(tabDelimitedLine[i].toString());
			        }
			        surfaceForms.put(tabDelimitedLine[0], set);
	
				}
				
				System.out.println("Loaded Surface Forms for " + surfaceForms.size() + " Resources...");
				
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					if (br != null)br.close();
				} catch (IOException ex) {
					ex.printStackTrace();
				}
			}
			
		} 
		
//		load redirects if location is available
		if(locationRD!=null){
		
			try {
				System.out.println("Loading Redirects...");
				
				int sfSize = surfaceForms.size();
				String sCurrentLine;
				br = new BufferedReader(new FileReader(locationRD));
	
				while ((sCurrentLine = br.readLine()) != null) {
					
//					because the file is 'Tab Delimited', we split the every line with 'tab' to separate entity from its redirects
					String[] tabDelimitedLine = sCurrentLine.split("\\t");
			    
					if(tabDelimitedLine.length < 2){
			        	continue;
			        }
			        else{
			        	if(tabDelimitedLine[0].isEmpty() || tabDelimitedLine[1].isEmpty())
			        		continue;
			        	else{
			        		HashSet<String> set = new HashSet<String>();
			        		set.add(tabDelimitedLine[1]);
			        		if(surfaceForms.containsKey(tabDelimitedLine[0])){
			        			surfaceForms.get(tabDelimitedLine[0]).add(tabDelimitedLine[1]);
			        		} else{
			        			surfaceForms.put(tabDelimitedLine[0], set);					        		
			        		}
			        	}
			        		
			        }
				}
				
				System.out.println("Loaded Redirecst for " + (surfaceForms.size() - sfSize) + " Resources...");
				
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					if (br != null)br.close();
				} catch (IOException ex) {
					ex.printStackTrace();
				}
			}	
		}
		
		setSurfaceForms(surfaceForms);
		
	}
	
//	method to normalize string value (label). this method is inherited from project 'Normalisation'
	public static String getNormalizedLabel(MatchableTableRow record){
		return StringNormalizer.normaliseValue((String)record.get(rdfsLabel), false);
	}
	
//	for testing purpose
	public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
		SurfaceForms sf = new SurfaceForms(new File("C:\\Users\\Sanikumar\\Desktop\\runT2KWDI\\SFs\\SFs.txt"), null);
		sf.loadSurfaceForms();
	}
}
