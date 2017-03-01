package de.uni_mannheim.informatik.dws.t2k.utils;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;

public class DataLogger {

	private boolean isEnabled;
	
	public DataLogger()
	{
		isEnabled=true;
	}

	public void logMap(Map<?,?> map, String name)
	{
		if(isEnabled)
		{
			try {
				BufferedWriter w = new BufferedWriter(new FileWriter(name + ".csv"));
				
				w.write("key\tvalue\n");
				
				for(Entry<?, ?> e : map.entrySet())
					w.write(e.getKey().toString() + "\t" + e.getValue().toString() + "\n");
				
				w.close();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
	}
	
	public void logMapMap(Map<?,?> map, String name)
	{
		if(isEnabled)
		{
			try {
				BufferedWriter w = new BufferedWriter(new FileWriter(name + ".csv"));
				
				w.write("key1\tkey2\tvalue\n");
				
				for(Entry<?, ?> e0 : map.entrySet())
					for(Entry<?,?> e : ((Map<?,?>)e0.getValue()).entrySet())
						w.write(e0.getKey().toString() + "\t" + e.getKey().toString() + "\t" + e.getValue().toString() + "\n");
				
				w.close();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
	}
}
