package de.uni_mannheim.informatik.dws.t2k.utils.io;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;


public class FileSet {

	private String URL;
	private String id;
	private File jsonFile;
	private List<File> csvFiles;
	private File htmlFile;
	
	public FileSet copy()
	{
		FileSet fs = new FileSet();
		
		fs.id = getId();
		fs.jsonFile = new File(getJsonFile().getAbsolutePath());
		fs.htmlFile = new File(getHtmlFile().getAbsolutePath());
		
		fs.csvFiles = new LinkedList<File>();
		for(File f : getCsvFiles())
			fs.csvFiles.add(new File(f.getAbsolutePath()));
				
		return fs;
	}
	
	public String getURL() {
		return URL;
	}
	
	public File getJsonFile() {
		return jsonFile;
	}
	
	public List<File> getCsvFiles() {
		return csvFiles;
	}
	
	public File getHtmlFile() {
		return htmlFile;
	}
	
	public String getId() {
		return id;
	}
		
	protected FileSet()
	{
		
	}
	
	public FileSet(File jsonFile)
	{
		this.jsonFile = jsonFile;
		
		this.id = jsonFile.getName().split("\\_")[0];
		
		File[] csvFiles = jsonFile.getParentFile().listFiles(new FilenameFilter() {
			
			public boolean accept(File dir, String name) {
				return name.matches(id + "\\_.+\\.csv");
			}
		});
		
		if(csvFiles!=null)
		{
			
			this.csvFiles = new ArrayList<File>(csvFiles.length);
			
			for(File f : csvFiles)
				this.csvFiles.add(f);
		}
		
		File[] htmlFile = jsonFile.getParentFile().listFiles(new FilenameFilter() {
			
			public boolean accept(File dir, String name) {
				return name.matches("^" + id + "[^\\.]+$");
			}
		});
		
		if(htmlFile!=null)
		{
			this.htmlFile = htmlFile[0]; 
		}
	}
	
	public static List<FileSet> getFileSets(File folder)
	{
		List<FileSet> list = new LinkedList<FileSet>();
		
		File[] jsonFiles = folder.listFiles(new FilenameFilter() {

			public boolean accept(File dir, String name) {
				return name.endsWith(".json");
			}
		});

		if(jsonFiles!=null)
		{
			for (File json : jsonFiles) {
				FileSet fs = new FileSet(json);
				list.add(fs);
			}
		}
		
		return list;
	}

	protected String readJsonStringFromFile(String file) {
		String jsonString = "";
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader(file));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		try {
			//
			String line = "";
			while ((line = br.readLine()) != null) {
				jsonString += line.replace(":NaN", "0.0");
			}
			br.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return jsonString;
	}

}
