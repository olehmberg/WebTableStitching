package de.uni_mannheim.informatik.dws.t2k.datatypes;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * @author petar
 *
 */
public class SimpleStringProcessor {

	public static String mkString(Set<String> list, String separator) {
		StringBuilder s = new StringBuilder();
		Iterator<String> it = list.iterator();
		if (it.hasNext()) {
			s.append(it.next());
		}
		while (it.hasNext()) {
			s.append(separator).append(it.next());
		}
		return s.toString();
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Map<String, Integer> months = DataTypesConfig.months;
		System.out.println(mkString(months.keySet(), "|"));
	}
}
