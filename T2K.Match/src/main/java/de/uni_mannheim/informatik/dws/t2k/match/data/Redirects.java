package de.uni_mannheim.informatik.dws.t2k.match.data;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;

@Deprecated
public class Redirects implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private File location;
	
	public Redirects(File location){
		this.location = location;
		if(location==null || !location.exists()) {
			redirects = new HashMap<>();
		}
	}
	
	private static transient HashMap<String, String> redirects = null;

	public String getRedirect(String forLabel) {
		return redirects==null ? null : redirects.get(forLabel);
	}
	protected void setRedirects(HashMap<String, String> redirects) {
		Redirects.redirects = redirects;
	}
	
	public void loadIfRequired() {
		if(redirects==null) {
			synchronized (this) {
				if(redirects==null) {
					loadRedirects();
				}
			}
		}
	}
	
	public void loadRedirects(){
		
		redirects = new HashMap<>();
		
		BufferedReader br = null;

		try {
			System.out.println("Loading Redirects...");
			
			String sCurrentLine;
			br = new BufferedReader(new FileReader(location));

			while ((sCurrentLine = br.readLine()) != null) {
				String[] tabDelimitedLine = sCurrentLine.split("\\t");
		        if(tabDelimitedLine.length < 2){
		        	continue;
		        }
		        else{
		        	if(tabDelimitedLine[0].isEmpty() || tabDelimitedLine[1].isEmpty())
		        		continue;
		        	else
		        		redirects.put(tabDelimitedLine[0], tabDelimitedLine[1]);
		        }
			}
			
			System.out.println("Loaded Redirecst for " + redirects.size() + " Resources...");
			
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}	
		setRedirects(redirects);
		
	}

//	for testing purpose
	public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
		
		Redirects r = new Redirects(new File("C:\\Users\\Sanikumar\\Desktop\\runT2KWDI\\redirects"));
		r.loadRedirects();
	}
}
