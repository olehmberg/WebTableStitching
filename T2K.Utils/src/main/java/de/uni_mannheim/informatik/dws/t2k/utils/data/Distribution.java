package de.uni_mannheim.informatik.dws.t2k.utils.data;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import de.uni_mannheim.informatik.dws.t2k.utils.query.Func;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;

public class Distribution<T> {

	private Map<T, Integer> counts = new HashMap<>();
	private int sumOfCounts = 0;
	
	public void add(T element) {
		Integer cnt = getNonNull(element);
		
		counts.put(element, cnt+1);
		
		sumOfCounts++;
	}
	
	protected Integer getNonNull(T element) {
		Integer cnt = counts.get(element);
		
		if(cnt==null) {
			cnt = 0;
		}
		
		return cnt;
	}
	
	public int getFrequency(T element) {
		return getNonNull(element);
	}
	
	public double getRelativeFrequency(T element) {
		return (double)getFrequency(element) / (double)sumOfCounts;
	}
	
	public Set<T> getElements() {
		return counts.keySet();
	}
	
	public int getPopulationSize() {
		return sumOfCounts;
	}
	
	public int getNumElements() {
		return counts.size();
	}
	
	public T getMode() {
		T maxElem = null;
		int maxFreq = 0;
		
		for(T elem : counts.keySet()) {
			int freq = getFrequency(elem);
			if(freq>maxFreq) {
				maxFreq=freq;
				maxElem=elem;
			}
		}
		
		return maxElem;
	}
	
	public String format() {
		StringBuilder sb = new StringBuilder();
		int len = 0;
		for(T elem : counts.keySet()) {
			len = Math.max(len, elem.toString().length());
		}

		List<T> sortedElements = Q.sort(counts.keySet(), new Comparator<T>() {

			@Override
			public int compare(T o1, T o2) {
				return -Integer.compare(getFrequency(o1), getFrequency(o2));
			}
		});
		
		for(T elem : sortedElements) {
			sb.append(String.format("%-" + (len+10) + "s%d\n", elem, getFrequency(elem)));
		}

		return sb.toString();
	}
	
	public static <T> Distribution<T> fromCollection(Collection<T> data) {
		Distribution<T> d = new Distribution<>();
		
		for(T elem : data) {
			d.add(elem);
		}
		
		return d;
	}
	
	public static <T, TDist> Distribution<TDist> fromCollection(Collection<T> data, Func<TDist, T> statistic) {
		Distribution<TDist> d = new Distribution<>();
		
		for(T elem : data) {
			d.add(statistic.invoke(elem));
		}
		
		return d;
	}
	
	public boolean isUniform() {
		int freq = -1;
		for(T key : counts.keySet()) {
			if(freq==-1) {
				freq = counts.get(key);
			} else {
				if(freq!=counts.get(key)) {
					return false;
				}
			}
		}
		return true;
	}
}
