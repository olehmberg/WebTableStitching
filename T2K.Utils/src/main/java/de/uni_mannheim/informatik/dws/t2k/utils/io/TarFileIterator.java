package de.uni_mannheim.informatik.dws.t2k.utils.io;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.IOUtils;

public class TarFileIterator extends FileIterator {

	private TarArchiveInputStream archive;
	private TarArchiveEntry currentEntry;
	private InputStream currentStream;
	
	public TarFileIterator(String path, boolean extractTars,
			boolean extractGzips) throws FileNotFoundException, IOException {
		super(path, extractTars, extractGzips);
		
		getFiles().clear();

		if(path.endsWith("gz"))
			archive = new TarArchiveInputStream(new GzipCompressorInputStream(new BufferedInputStream(new FileInputStream(path))));
		else
			archive = new TarArchiveInputStream(new FileInputStream(path));		
	}

	@Override
	protected void next() {
		try {
			currentEntry = archive.getNextTarEntry();
			currentStream = null;
		} catch (IOException e) {
			e.printStackTrace();
			currentEntry = null;
			currentStream = null;
		}
	}
	
	@Override
	public File getCurrentFile() {
		if(currentEntry==null)
			return null;
		else
		{
			// currentEntry.getFile() is null ...
			return new File(currentEntry.getName());
		}
	}
	
	@Override
	public InputStream getCurrentStream() {
		if(currentStream == null)
		{
			byte[] content = new byte[(int) currentEntry.getSize()];
			try {
				archive.read(content, 0, content.length);
			} catch (IOException e) {
				e.printStackTrace();
				return null;
			}
			
			currentStream = new ByteArrayInputStream(content);
		}
		return currentStream;
	}
	
	@Override
	public void close() {
		super.close();
		
		try {
			archive.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws FileNotFoundException, IOException {
        if(args.length!=2) {
            System.err.println("Usage: <archive name> <output dir>");
            return;
        }
        
        TarFileIterator it = new TarFileIterator(args[0], true, true);
        
        InputStream is = null;
        
        File fOut = new File(args[1]);
        
        if(!fOut.exists()) {
            System.out.println("Creating output directory " + fOut.getAbsolutePath());
            fOut.mkdirs();
        }
        
        while((is = it.getNext()) != null) {
            
            File f = new File(fOut, it.getCurrentFile().getName());
            
            System.out.println("Extracting " + f.getName());
            
            IOUtils.copy(is, new FileOutputStream(f));
            is.close();
        }
        
        it.close();
        
        System.out.println("done.");
    }
}
