package de.uni_mannheim.informatik.dws.t2k.utils.io;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import de.uni_mannheim.informatik.dws.t2k.utils.data.Pair;

public abstract class DataStorageUtils2<T, U> {

	public static void saveAnyMapCollection(Map<?, Collection<?>> data, String file) throws IOException
	{		
		BufferedWriter w = new BufferedWriter(new FileWriter(file));
		
		for(Object key : data.keySet())
		{
			w.write(key.toString());
			w.write("\t");
			
			boolean first=true;
			for(Object value : data.get(key))
			{
				if(!first)
					w.write(",");
				
				w.write(value.toString().replace(",", "\\,"));
				
				first = false;
			}
			
			w.write("\n");
		}
		
		w.close();
	}
	
	public void saveMapCollection(Map<T, Collection<U>> data, String file) throws IOException
	{
		BufferedWriter w = new BufferedWriter(new FileWriter(file));
		
		for(Object key : data.keySet())
		{
			if(data.get(key)!=null && data.get(key).size()>0)
			{
				w.write(key.toString());
				w.write("\t");
				
				boolean first=true;
				for(Object value : data.get(key))
				{
					if(!first)
						w.write(",");
					
					w.write(value.toString().replace(",", "\\,"));
					
					first = false;
				}
				
				w.write("\n");
			}
		}
		
		w.close();
	}
	
	public abstract Map<T, Collection<U>> createMapCollection();
	//public abstract Collection<U> createCollection1();
	public abstract Collection<U> createCollection2();
	public abstract Collection<Pair<T, U>> createPairCollection();
	public abstract T createValueFromString1(String value);
	public abstract U createValueFromString2(String value);
	
	public Map<T, Collection<U>> loadMapCollection(String file) throws IOException
	{
		Map<T, Collection<U>> result = createMapCollection();
		
		BufferedReader r = new BufferedReader(new FileReader(file));
		
		String line = null;
		
		while((line = r.readLine()) != null)
		{
			String[] parts = line.split("\t");
			
			Collection<U> col = createCollection2();
			
			if(parts.length>1)
			{
				String[] values = parts[1].split(",");
				
				for(String s : values)
					col.add(createValueFromString2(s));
			}
			result.put(createValueFromString1(parts[0]), col);
		}
		
		r.close();
		
		return result;
	}
	
	public Collection<Pair<T, U>> loadPairsForHeader(String file, String header) throws IOException
	{
		Collection<Pair<T, U>> result = createPairCollection();
		
		BufferedReader r = new BufferedReader(new FileReader(file));
		
		String line = null;
		String currentHeader = null;
		
		while((line = r.readLine()) != null)
		{
			if(!line.contains("\t"))
				currentHeader = line;
			else if(header.equals(currentHeader))
			{
				String[] parts = line.split("\t");
				
				result.add(new Pair<T, U>(createValueFromString1(parts[0]), createValueFromString2(parts[1])));
			}
		}
		
		r.close();
		
		return result;
	}
}
