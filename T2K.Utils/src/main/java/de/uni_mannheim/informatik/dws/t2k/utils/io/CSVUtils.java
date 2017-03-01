package de.uni_mannheim.informatik.dws.t2k.utils.io;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collection;

import au.com.bytecode.opencsv.CSVReader;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;

public class CSVUtils {

    protected static boolean verbose = false;
    public static void setVerbose(boolean verbose) {
        CSVUtils.verbose = verbose;
    }
    public static boolean isVerbose() {
        return verbose;
    }
    
    public static Collection<String[]> readCSV(String path) {
        return readCSV(path, null);
    }
    
	public static Collection<String[]> readCSV(String path, String delimiter)
	{
		Collection<String[]> lst = null;
		
		try {
		    if(isVerbose()) {
		        System.out.println("measuring size of " + path);
		    }
			BufferedReader r = new BufferedReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));
			int lines=0;
			while(r.readLine()!=null)
			{
				lines++;
			}
			r.close();
			
			lst = new ArrayList<String[]>(lines);
			
			if(isVerbose()) {
			    System.out.println("reading " + lines + " lines from " + path);
			}
			CSVReader reader = null;
			
			if(delimiter==null) {
			    reader = new CSVReader(new InputStreamReader(new FileInputStream(path), "UTF-8"));
			} else {
			    reader = new CSVReader(new InputStreamReader(new FileInputStream(path), "UTF-8"), delimiter.charAt(0));
			}
			
			String[] values = null;
			
			while((values = reader.readNext()) != null)
			{
				lst.add(values);
			}
			
			reader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		return lst;
	}
        
        public static InputStream toUTF8InputStream(String str) {
        InputStream is = null;
        try {
          is = new ByteArrayInputStream(str.getBytes("UTF-8"));
        } catch (UnsupportedEncodingException e) {
          // UTF-8 should always be supported
          throw new AssertionError();
        }
        return is;
      }
	
}
