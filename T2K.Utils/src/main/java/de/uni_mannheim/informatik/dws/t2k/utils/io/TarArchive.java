package de.uni_mannheim.informatik.dws.t2k.utils.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.LinkedList;
import java.util.List;
import java.util.zip.GZIPInputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.io.IOUtils;

public class TarArchive {

	private String path;
	private String archiveName;
	private List<File> files;
	private File extracted;
	
	public List<File> getFiles() {
		return files;
	}
	
	public File getExtracted() {
		return extracted;
	}
	
	public TarArchive(String path)
	{
		this.path = path;
		files = new LinkedList<File>();
	}
	
	public void deleteExtracted()
	{
		for(File f : files)
			f.delete();
		extracted.delete();
	}
	
	public File extract() throws Exception {
		File i = new File(path);

		// this is the extraction folder that needs to be returned
		File outputDir = null;
		String fileName = i.toString();
		String tarFileName = fileName;
                boolean zipped = false;
		
		if(fileName.endsWith(".gz"))
		{
			tarFileName = fileName.substring(0, fileName.lastIndexOf("."));
                        zipped =true;
			
			// extract gz
			FileInputStream instream = new FileInputStream(fileName);
			GZIPInputStream ginstream = new GZIPInputStream(instream);
			FileOutputStream outstream = new FileOutputStream(tarFileName);
			byte[] buf = new byte[1024];
			int len;
			while ((len = ginstream.read(buf)) > 0) {
				outstream.write(buf, 0, len);
			}
                        instream.close();
			ginstream.close();
			outstream.close();
		}
		
		// There should now be tar files in the directory
		// extract specific files from tar
		TarArchiveInputStream myTarFile = new TarArchiveInputStream(
				new FileInputStream(tarFileName));
		TarArchiveEntry entry = null;
		int offset;
		FileOutputStream outputFile = null;

		// remove .tar from the archive name
		archiveName = tarFileName.substring(0, tarFileName.lastIndexOf("."));
                
		// create the output folder
                File archi = new File(archiveName);
                if(archi.isAbsolute()) {
                    outputDir = archi;
                    outputDir.mkdir();
                }
                else {
                    outputDir = new File(i.getParent(), archiveName);
                    outputDir.mkdir();
                }
		
		// read every single entry in TAR file
		while ((entry = myTarFile.getNextTarEntry()) != null) {
			File current = new File(outputDir,entry.getName());
			if (!current.getParentFile().exists()) {
				current.getParentFile().mkdirs();
			}
			
			// if the entry in the tar is a directory, it needs to be
			// created, only files can be extracted
			if (entry.isDirectory()) {
				current.mkdirs();
			} else {
				byte[] content = new byte[(int) entry.getSize()];
				offset = 0;
				myTarFile.read(content, offset, content.length - offset);
				outputFile = new FileOutputStream(current);
				IOUtils.write(content, outputFile);
				outputFile.close();
				files.add(new File(current.getAbsolutePath()));
			}
		}
		
		// close and delete the tar files, and delete the gz file
		myTarFile.close();                
		//i.delete();
                if(zipped) {
                    File tarFile = new File(tarFileName);
                    tarFile.delete();
                }
		//outputDir = new File(i.getAbsolutePath().replace(".tar.gz", ""));
		extracted = outputDir;
		return outputDir;
	}
		
	public static void main(String[] args) throws Exception {
		new TarArchive(args[0]).extract();
	}

}
