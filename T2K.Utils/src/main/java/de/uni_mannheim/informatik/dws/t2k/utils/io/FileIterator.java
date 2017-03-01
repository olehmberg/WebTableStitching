package de.uni_mannheim.informatik.dws.t2k.utils.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedList;
import java.util.Queue;
import java.util.zip.GZIPInputStream;

/***
 * Iterates all files under the path passed in the constructor (including subdirectories)
 * Extracts tars and gzips if requested
 * If tars are extracted, they are iterated like a normal subdirectory (note: the tar is kept in memory)
 * 
 * Usage:
 * There are more files as long as getNext() does not return null
 * use getCurrentFile(), getCurrentPath() and getCurrentStream() to access the files
 * To read any file, you should always use the stream that is returned by getNext()
 * 
 * @author Oliver
 *
 */
public class FileIterator {

	private File rootPath;
	private boolean extractTars;
	private boolean extractGzips;
	private Queue<File> files;
	private FileIterator currentIterator;
	private File currentFile;
	
	public boolean isExtractGzips() {
		return extractGzips;
	}
	
	public boolean isExtractTars() {
		return extractTars;
	}
	
	protected Queue<File> getFiles() {
		return files;
	}
	
	protected FileIterator getCurrentIterator() {
		return currentIterator;
	}
	
	protected File getRootPath() {
		return rootPath;
	}
	
	protected void setCurrentIterator(FileIterator currentIterator) {
		this.currentIterator = currentIterator;
	}
	
	/***
	 * Constructs a new FileIterator for the given path
	 * @param path the path to iterate
	 * @param extractTars extract tar files and treat them as normal directories
	 * @param extractGzips extract gzip files before returning them
	 */
	public FileIterator(String path, boolean extractTars, boolean extractGzips)
	{
		rootPath = new File(path);
		this.extractTars = extractTars;
		this.extractGzips = extractGzips;
		files = new LinkedList<File>();
		
		if(rootPath.isDirectory())
		{
			File[] fileList =rootPath.listFiles();
			if(fileList!=null)
			{
				for(File f : fileList)
				{
					files.add(f);
				}
			}
		}
		else
		{
			files.add(rootPath);
		}
			
	}
	
	protected void next()
	{
		currentFile = files.poll();
	}
	
	/**
	 * get a File object representing the current file
	 * note: if the current file is from a tar archive, you cannot open the file using this object
	 * @return
	 */
	public File getCurrentFile()
	{
		return currentFile;
	}
	
	/**
	 * get the path of the current file
	 * note: if the current file is from a tar archive, the path is artificially constructed and does not exist on the file system
	 * @return
	 */
	public String getCurrentPath()
	{
		if(currentIterator==null)
			return rootPath + "/" + getCurrentFile().getName();
		else
			return currentIterator.getCurrentPath();
	}

	/**
	 * get an InputStream for the current file
	 * note: this is the raw stream, and no gzip extraction is performed. Use the stream returned by getNext() to have gzips extracted
	 * @return
	 */
	public InputStream getCurrentStream()
	{
		try {
			return new FileInputStream(currentFile);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public InputStream getNext()
	{
		if(currentIterator == null)
		{
			next();
		
			File current = getCurrentFile();
			
			if(current == null)
				return null; // there are no more files
			
			boolean isGzip = false;
			boolean isTar = false;
			if(current.getName().endsWith("tar.gz") || current.getName().endsWith("tgz"))
			{
				isGzip = true;
				isTar = true;
			}
			else if(current.getName().endsWith("gz"))
				isGzip = true;
			else if(current.getName().endsWith("tar"))
				isTar = true;
		
			// directories are iterated again ...
			if(current.isDirectory())
			{
				currentIterator = new FileIterator(current.getAbsolutePath(), extractTars, extractGzips);
			}
			else if(isTar && extractTars && (!isGzip || extractGzips))
			{
				// tars are iterated if requested
				// tar.gzs are iterated if requested
				// tar.gz is not treated as tar if extractGzips is false
				try {
					currentIterator = new TarFileIterator(current.getAbsolutePath(), extractTars, extractGzips);
				} catch (FileNotFoundException e) {
					e.printStackTrace();
					return null;
				} catch (IOException e) {
					e.printStackTrace();
					return null;
				}
			}
			else if(isGzip && extractGzips)
			{
				// gzips are extracted if requested
				try {
					return new GZIPInputStream(getCurrentStream());
				} catch (IOException e) {
					e.printStackTrace();
					return null;
				}
			}
			else
			{
				// if non of the previous cases apply, we just return the file
				return getCurrentStream();
			}
		}
			
		InputStream ret = currentIterator.getNext();
		
		if(ret==null)
		{
			currentIterator.close();
			currentIterator = null;
			return getNext();
		}
		else
			return ret;
	}
	
	public void close()
	{
		
	}
}
