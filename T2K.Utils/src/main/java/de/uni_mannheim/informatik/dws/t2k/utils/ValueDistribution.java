package de.uni_mannheim.informatik.dws.t2k.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class ValueDistribution {

	public static Map<Integer, Integer> generateCount(Map<?, Integer> values)
	{
		Map<Integer, Integer> result = new HashMap<Integer, Integer>();
		
		for(Integer i : values.values())
		{
			int cnt = 0;
			
			if(result.containsKey(i))
				cnt = result.get(i);
			
			result.put(i, cnt+1);
		}
		
		return result;
	}
	
	public static Object getMostFrequentValue(Map<?, Integer> values)
	{
		int max = Integer.MIN_VALUE;
		Object maxKey = null;
		
		for(Entry<?, Integer> e : values.entrySet())
		{
			int current = e.getValue();
			if(current>max)
			{
				max = current;
				maxKey = e.getKey();
			}
		}
		
		return maxKey;
	}
	
}
