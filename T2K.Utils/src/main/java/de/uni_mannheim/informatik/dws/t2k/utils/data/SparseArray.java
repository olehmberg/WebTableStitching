package de.uni_mannheim.informatik.dws.t2k.utils.data;

import java.util.ArrayList;

public class SparseArray<T> {

	private T[] values;
	private int[] indices;
	
	public T[] getValues() {
		return values;
	}
	
	public int[] getIndices() {
		return indices;
	}
	
	@SuppressWarnings("unchecked")
	public SparseArray(T[] sparseValues) {
		ArrayList<T> valueList = new ArrayList<>();
		ArrayList<Integer> indexList = new ArrayList<>();
		
		for (int i = 0; i < sparseValues.length; i++) {
			if(sparseValues[i]!=null) {
				valueList.add(sparseValues[i]);
				indexList.add(i);
			}
		}
		
//		values = valueList.toArray(new T[valueList.size()]);
//		indices = indexList.toArray(new int[indexList.size()]);
		
		values = (T[])valueList.toArray();
		indices = new int[indexList.size()];
		for (int i = 0; i < indexList.size(); i++) {
			indices[i] = indexList.get(i);
		}
	}
	
	public T get(int index) {
		return get(index, values, indices);
	}
	
	public static <T> T get(int index, T[] values, int[] indices) {
		int translatedIndex = translateIndex(index, indices);
		
		if(translatedIndex==-1) {
			return null;
		} else {
			return values[translatedIndex];
		}
	}
	
	public static int translateIndex(int index, int[] indices) {
		for (int i = 0; i < indices.length; i++) {
			if(indices[i]==index) {
				return i;
			}
		}
		
		return -1;
	}
}
