package de.uni_mannheim.informatik.dws.t2k.goldstandard;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import au.com.bytecode.opencsv.CSVReader;
import au.com.bytecode.opencsv.CSVWriter;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.t2k.match.data.WebTables;

public class GoldStandard {

	private File readLocation;
	private File writeLocation;
	private WebTables web;

	public GoldStandard(String readLocation, String writeLocation, WebTables web) {
		this.readLocation = new File(readLocation);
		this.writeLocation = new File(writeLocation);
		this.web = web;
	}
	
	public void convertOldGStoNewGS() throws IOException{
		HashMap<String, Integer> tableIndices = web.getTableIndices();
		HashMap<Integer, Integer> keyIndices = web.getKeyIndices();
		
		CSVWriter csvWriter = new CSVWriter(new OutputStreamWriter(new FileOutputStream(writeLocation)));
		
		List<File> instanceGS = null;
    	
    	if(readLocation.isDirectory()) {
    		instanceGS = Arrays.asList(readLocation.listFiles());
    	} else {
    		instanceGS = Arrays.asList(new File[] { readLocation});
    	}
    	
    	for(MatchableTableRow trow : web.getRecords().get()){
    		SecondForLoop:
    		for(File f : instanceGS){
    			if(tableIndices.get(f.getName()) != null){
    				int tableID = tableIndices.get(f.getName());
    				if(trow.getTableId()==tableID){
    					if(trow.get(keyIndices.get(tableID))!=null){
    						CSVReader csvReader = new CSVReader(new InputStreamReader(new FileInputStream(f)));
    			    		String[] values;
    			    		while((values = csvReader.readNext()) != null){
    			    			String dbResourceLink = values[0];
    			    			String entity = values[1].replaceAll("\\s", "");
    			    			if(trow.get(keyIndices.get(tableID)).toString().replaceAll("\\s", "").equals(entity)){
    	    						String[] rowGS = {trow.getIdentifier(), dbResourceLink, "TRUE"};
    	    	    				csvWriter.writeNext(rowGS);
    	    	    				csvReader.close();
    	    	    				break SecondForLoop;
    	    	    			} else{
    	    	    				continue;
    	    	    			}
    			    		}
    			    		csvReader.close();
    					}
    				}
    			}
    		}
    	}
    	
//    	for(File f : instanceGS){
//    		CSVReader csvReader = new CSVReader(new InputStreamReader(new FileInputStream(f)));
//    		String[] values;
//    		while((values = csvReader.readNext()) != null){
//    			String dbResourceLink = values[0];
//    			String entity = values[1].replaceAll("\\s", "");
//    			
//    			if(tableIndices.get(f.getName()) != null){
//	    			int tableID = tableIndices.get(f.getName());
//	    		
//	    			for(MatchableTableRow trow : web.getRecords().get()){
//	    				if(trow.getTableId()==tableID){
//	    					if(trow.get(keyIndices.get(tableID))!=null){
//	    						if(trow.get(keyIndices.get(tableID)).toString().replaceAll("\\s", "").equals(entity)){
//	    						String[] rowGS = {trow.getIdentifier(), dbResourceLink, "TRUE"};
//	    	    				csvWriter.writeNext(rowGS);
//	    	    				} else{
//	    	    					continue;
//	    	    				}	
//	    					}
//	    				}
//	    			}
//    			}
//    		}
//    		csvReader.close();
//    	}
    	
    	csvWriter.close();
	}
	
}
