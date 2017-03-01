package de.uni_mannheim.informatik.dws.t2k.utils.data;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MapUtils {

	public static <T> Integer increment(Map<T, Integer> map, T keyValue) {
		Integer cnt = map.get(keyValue);
		
		if(cnt==null) {
			cnt = 0;
		}
		
		map.put(keyValue, cnt+1);
		
		return cnt+1;
	}
	
	public static <T> void add(Map<T, Integer> map, T keyValue, int value) {
		Integer cnt = map.get(keyValue);
		
		if(cnt==null) {
			cnt = 0;
		}
		
		map.put(keyValue, cnt+value);
	}
	
	public static <T> T max(Map<T, Integer> map) {
		Integer iMax = null;
		T tMax = null;
		
		for(T t : map.keySet()) {
			Integer i = map.get(t);
			
			if(iMax==null || i>iMax) {
				iMax = i;
				tMax = t;
			}
		}
		
		return tMax;
	}
	
	/***
	 * returns the value for the specified keyValue from the map. If no entry exists, defaultValue is added to the map and returned.
	 * @param map
	 * @param keyValue
	 * @param defaultValue
	 * @return
	 */
	public static <T, U> U get(Map<T, U> map, T keyValue, U defaultValue) {
		U val = map.get(keyValue);
		
		if(val==null) {
			map.put(keyValue, defaultValue);
			return defaultValue;
		} else {
			return val;
		}
	}
	
	public static <K, V> List<Map.Entry<K, V>> sort(Map<K, V> map, Comparator<Map.Entry<K, V>> comparator) {
		ArrayList<Map.Entry<K, V>> sorted = new ArrayList<>(map.size());
		for(Map.Entry<K, V> entry : map.entrySet()) {
			sorted.add(entry);
		}
		Collections.sort(sorted, comparator);
		return sorted;
	}
	
	/**
	 * inverts a key-value map such that all values will become keys and the keys will be the values.
	 * in case of duplicates, values are overwritten
	 * @param map
	 * @return
	 */
	public static <K, V> Map<V, K> invert(Map<K, V> map) {
		HashMap<V, K> inverted = new HashMap<>();
		
		for(K key : map.keySet()) {
			inverted.put(map.get(key), key);
		}
		
		return inverted;
	}
}
