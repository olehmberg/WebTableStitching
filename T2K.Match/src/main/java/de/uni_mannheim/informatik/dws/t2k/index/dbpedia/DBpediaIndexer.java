package de.uni_mannheim.informatik.dws.t2k.index.dbpedia;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import org.apache.lucene.index.IndexWriter;

import de.uni_mannheim.informatik.dws.t2k.index.IIndex;
import de.uni_mannheim.informatik.dws.t2k.index.io.DefaultIndex;
import de.uni_mannheim.informatik.dws.t2k.normalisation.StringNormalizer;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.t2k.match.data.SurfaceForms;
import de.uni_mannheim.informatik.dws.t2k.webtables.Table;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableRow;
import de.uni_mannheim.informatik.dws.t2k.webtables.parsers.LodCsvTableParser;


public class DBpediaIndexer {

	/**
	 * Write records (represented as a document) to lucene.
	 * @param index
	 * 			the IIndex object representing location (or in memory index) to store index (must not be null)
	 * @param rows
	 * 			the records to be indexed (must not be null)
	 * @param classIndices
	 * 			the class indices mapping table id to its name (must not be null)
	 * @param sf
	 * 			the surface forms (must not be null)
	 */
	public void indexInstances(IIndex index, Collection<MatchableTableRow> rows, Map<Integer, String> classIndices, SurfaceForms sf)
    {	

		IndexWriter writer = index.getIndexWriter();
        
        long cnt=0;
        
        for(MatchableTableRow row : rows) {
            DBpediaIndexEntry e = new DBpediaIndexEntry();
            
            // URI is always in the first column
            e.setUri((String)row.get(0));
            
            // key is always in the second column
            String label = (String)row.get(1);
                       
            if(label!=null) {
	            // removes artifacts from the CSV format
	            //String labelClean = StringCleaner.cleanString(label, false);
	            
	            // normalises the value to improve lookup results
	            //label = StringNormaliser.normalise(labelClean, true);
            	label = StringNormalizer.normalise(StringNormalizer.normaliseValue(label, true), true);
	            //System.out.println(String.format("before: %s \t cleaned: %s \t after: %s", t.getKey().getValues().get(i).toString(), labelClean, label));
	            
	            e.setLabel(label);
	            e.setClass_label(classIndices.get(row.getTableId()));
	            
//	            System.out.println((String)row.get(0) + "  " + (String)row.get(1) + "  " + classIndices.get(row.getTableId()));
	            
	            try {
	            	writer.addDocument(e.createDocument());
	                for(String sForm : sf.getSurfaceForms(SurfaceForms.getNormalizedLabel(row))) {
//			                	sForm = StringNormalizer.normalise(StringNormalizer.normaliseValue(sForm, true), true);
	                	e.setLabel(sForm);
	                	writer.addDocument(e.createDocument());	
	                }
	            } catch (IOException e1) {
	                e1.printStackTrace();
	            }
            }
            cnt++;
            
            if(cnt%100000==0)
            {
                System.out.println("Indexed " + cnt + " items.");
            }
        }
        
        System.out.println("Indexed " + cnt + " items.");
        
        index.closeIndexWriter();
    }
	
	   public void indexInstances(IIndex index, Table t)
	    {
	       if(!t.hasKey()) {
	           System.out.println("No key!");
	           return;
	       } else {
	           System.out.println(String.format("Key is [%d] %s", t.getKeyIndex(), t.getKey().getHeader()));
	       }
	       
	        IndexWriter writer = index.getIndexWriter();
	        
	        long cnt=0;
	        
	        for(TableRow row : t.getRows()) {
	            DBpediaIndexEntry e = new DBpediaIndexEntry();
	            e.setUri((String)row.get(0));
                
	            String label = (String)row.getKeyValue();
                
	            if(label!=null) {
		            // removes artifacts from the CSV format
//		            String labelClean = StringCleaner.cleanString(label, false);
		            
		            // normalises the value to improve lookup results
//		            label = StringNormaliser.normalise(labelClean, true);
		            label = StringNormalizer.normalise(StringNormalizer.normaliseValue(label, true), true);
		                                
		            //System.out.println(String.format("before: %s \t cleaned: %s \t after: %s", t.getKey().getValues().get(i).toString(), labelClean, label));
		            
		            e.setLabel(label);
		            e.setClass_label(t.getPath().replace(".csv", ""));
		            
		            try {
		                writer.addDocument(e.createDocument());
		            } catch (IOException e1) {
		                e1.printStackTrace();
		            }
	            }
	            cnt++;
	            
	            if(cnt%100000==0)
	            {
	                System.out.println("Indexed " + cnt + " items.");
	            }
	        }
	        
	        System.out.println("Indexed " + cnt + " items.");
	        
	        index.closeIndexWriter();
	    }
	   
	   public static void main(String[] args) throws FileNotFoundException, IOException, ClassNotFoundException {
        DBpediaIndexer indexer = new DBpediaIndexer();
        LodCsvTableParser lodParser = new LodCsvTableParser();
//        r.setUseUnitDetection(true);
        
        DefaultIndex idx = new DefaultIndex(args[0]);
        
        if(new File(args[1]).isDirectory()) {
        
            System.out.println("Processing directory");
            
            ArrayList<Table> tables = new ArrayList<Table>(new File(args[1]).list().length);
            
            for(File f : new File(args[1]).listFiles()) {
                try {
                    System.out.println(f.getName());
                    Table t = lodParser.parseTable(f);
                    tables.add(t);
                    indexer.indexInstances(idx, t);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        
        } 
        
        System.out.println("done.");
    }
}
