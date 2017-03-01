package de.uni_mannheim.informatik.dws.t2k.datatypes;

import java.util.HashMap;
import java.util.Map;

/**
 * @author petar
 *
 */
public class DataTypesConfig {
	public static final Map<String, Integer> months = new HashMap<String, Integer>() {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		{
			put("january", 1);
			put("february", 2);
			put("march", 3);
			put("april", 4);
			put("may", 5);
			put("june", 6);
			put("july", 7);
			put("august", 8);
			put("september", 9);
			put("october", 10);
			put("november", 11);
			put("december", 12);

		}
	};
	public static final Map<String, Integer> era = new HashMap<String, Integer>() {
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		{
			put("BCE", 1);
			put("BC", 1);
			put("CE", 1);
			put("AD", 1);
			put("AC", 1);
			put("CE", 1);

		}
	};

	public static String cardinalityRegex = "st|nd|rd|th";
}
