package de.uni_mannheim.informatik.dws.t2k.utils.data.string;

import java.util.HashMap;
import java.util.Map;

public class StringCache {

	private static Map<String, String> cache = new HashMap<>();
	
	public static String get(String s) {
		if(s==null) {
			return null;
		} else {
			String s2 = cache.get(s);
			if(s2==null) {
				// make sure that there is no longer string referenced by the value
				// http://www.javamex.com/tutorials/memory/string_memory_usage.shtml
				s2 = new String(s);
				cache.put(s2, s2);
				return s2;
			} else {
				return s2;
			}
		}
	}

	public static void clear() {
		cache.clear();
	}
}
