package de.uni_mannheim.informatik.dws.t2k.utils;

import java.util.Collection;

public class StringUtils {

	public static String join(Collection<?> values, String delimiter)
	{
		StringBuilder sb = new StringBuilder();
		
		boolean first=true;
		for(Object value : values)
		{
			if(!first)
				sb.append(delimiter);
			
			if(value!=null) {
				sb.append(value.toString());
			} else {
				sb.append("null");
			}
			
			first = false;
		}
		
		return sb.toString();
	}
	
	public static String join(Object[] values, String delimiter)
	{
		StringBuilder sb = new StringBuilder();
		
		boolean first=true;
		for(Object value : values)
		{
			if(!first)
				sb.append(delimiter);
			
			if(value!=null) {
				sb.append(value.toString());
			} else {
				sb.append("null");
			}
			
			first = false;
		}
		
		return sb.toString();
	}
	
	/**
	 * Checks if the string passed as first parameter contains any of the other strings passed as second parameter
	 * @param value
	 * @param testValues
	 * @return
	 */
	public static boolean containsAny(String value, Collection<String> testValues)
	{
		if(value==null)
			return false;
		
		for (String s : testValues)
			if (value.contains(s))
				return true;
		return false;
	}
}
