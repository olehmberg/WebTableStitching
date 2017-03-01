package de.uni_mannheim.informatik.dws.t2k.utils.io;

import java.io.BufferedWriter;
import java.io.FileWriter;

public class SynchronizedTextWriter 
extends SynchronizedWriter<String>
{

	private BufferedWriter writer;
	private long length=0;

   public SynchronizedTextWriter(String file) throws Exception {
        super(file);
    }
	
	public SynchronizedTextWriter(String file, boolean append) throws Exception {
		super(file, append);
	}

	@Override
	protected void createWriter(String file, boolean append) throws Exception {
		writer = new BufferedWriter(new FileWriter(file, append));
		length=0;
	}

	@Override
	public long getApproximateLength() throws Exception {
		if(writer!=null)
			return length;
		else
			return 0;
					
	}
	
	@Override
	protected void writeData(String data) throws Exception {
		String s = data + "\n";
		writer.write(s);
		
		length += s.length();
	}

	@Override
	protected void flushWriter() throws Exception {
		writer.flush();
	}

	@Override
	protected void closeWriter() throws Exception {
		writer.close();
	}

}
