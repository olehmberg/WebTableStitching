package de.uni_mannheim.informatik.dws.t2k.datatypes;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * @author petar
 * 
 */
public class DateTimeParser {

	public static Map<String, Integer> months = DataTypesConfig.months;
	public static Map<String, Integer> eraStr = DataTypesConfig.era;
	public static String cardinalityRegex = DataTypesConfig.cardinalityRegex;
	// public static val templates =
	// DateTimeParserConfig.templateDateMap.getOrElse(language, Map())

	// parse logic configurations

	public static String monthRegex = SimpleStringProcessor.mkString(
			months.keySet(), "|");
	public static String eraRegex = SimpleStringProcessor.mkString(
			eraStr.keySet(), "|");

	public static String prefix = "";
	public static String postfix = "";

	// catch dates like: "8 June 07" or "07 June 45"
	public static String DateRegex1 = ("(?iu)" + prefix + "([0-9]{1,2})\\s*("
			+ monthRegex + ")\\s*([0-9]{2})(?!\\d)\\s*(?!\\s)(?!" + eraRegex
			+ ")\\.*" + postfix);

	// catch dates like: "[[29 January]] [[300 AD]]",
	// "[[23 June]] [[2008]] (UTC)", "09:32, 6 March 2000 (UTC)" or
	// "3 June 1981"
	public static String DateRegex2 = ("(?iu)" + prefix
			+ "(?<!\\d)\\[?\\[?([0-9]{1,2})(\\.|" + cardinalityRegex
			+ ")?\\s*(" + monthRegex
			+ ")\\]?\\]?,? \\[?\\[?(-?[0-9]{1,4})\\s*(" + eraRegex
			+ ")?\\]?\\]?(?!\\d)" + postfix);

	// catch dates like: "[[January 20]] [[1995 AD]]",
	// "[[June 17]] [[2008]] (UTC)" or "January 20 1995"
	public static String DateRegex3 = ("(?iu)"
			+ prefix
			+ "\\[?\\[?("
			+ monthRegex
			+ ")\\s*,?\\s+([0-9]{1,2})\\]?\\]?\\s*[.,]?\\s+\\[?\\[?([0-9]{1,4})\\s*("
			+ eraRegex + ")?\\]?\\]?" + postfix);

	// catch dates like: "24-06-1867", "24/06/1867" or "bla24-06-1867bla"
	public static String DateRegex4 = ("(?iu)"
			+ prefix
			+ "(?<!\\d)([0-9]{1,2}+)[-/\\s\\.]([0-9]{1,2}+)[-/\\s\\.]([0-9]{3,4}+)(?!\\d)" + postfix);

	// catch dates like: "24-june-1867", "24/avril/1867" or
	// "bla24|juillet|1867bla"
	public static String DateRegex5 = ("(?iu)" + prefix
			+ "(?<!\\d)([0-9]{1,2}+)[-/\\|](" + monthRegex
			+ ")[-/\\|]([0-9]{3,4}+)(?!\\d)" + postfix);

	// catch dates like: "1990 06 24", "1990-06-24", "1990/06/24" or
	// "1977-01-01 00:00:00.000000"
	public static String DateRegex6 = ("(?iu)" + prefix + "(?<!\\d)([0-9]{3,4})[-/\\s\\.]([0-9]{1,2})[-/\\s\\.]([0-9]{1,2})(?!\\d).*");

	// catch dates like: "20 de Janeiro de 1999", "[[1º de Julho]] de [[2005]]"
	public static String DateRegex7 = ("(?iu)" + prefix
			+ "(?<!\\d)\\[?\\[?([0-9]{1,2})(\\.|" + cardinalityRegex
			+ ")?\\s*d?e?\\s*(" + monthRegex
			+ ")\\]?\\]?\\s*d?e?\\s*\\[?\\[?([0-9]{0,4})\\s*?\\]?\\]?(?!\\d)" + postfix);

	public static String DayMonthRegex1 = ("(?iu)" + prefix + "(" + monthRegex
			+ ")\\]?\\]?\\s*\\[?\\[?([1-9]|0[1-9]|[12][0-9]|3[01])(?!\\d)" + postfix);

	public static String DayMonthRegex2 = ("(?iu)" + prefix
			+ "(?<!\\d)([1-9]|0[1-9]|[12][0-9]|3[01])\\s*(" + cardinalityRegex
			+ ")?\\]?\\]?\\s*(of)?\\s*\\[?\\[?(" + monthRegex + ")\\]?\\]?" + postfix);

	public static String MonthYearRegex = ("(?iu)" + prefix + "(" + monthRegex
			+ ")\\]?\\]?,?\\s*\\[?\\[?([0-9]{1,4})\\s*(" + eraRegex + ")?" + postfix);
	public static String MonthYearRegex1 = ("(?iu)" + prefix
			+ "([0-9]{1,2}+)[-/\\s\\.]\\[?\\[?([0-9]{1,4})\\s*(" + eraRegex
			+ ")?" + postfix);

	public static String YearRegex = ("(?iu)" + prefix
			+ "(?<![\\d\\pL\\w])(-?\\d{1,4})(?!\\d)\\s*(" + eraRegex + ")?" + postfix);

	public static void main(String[] args) {
		System.out.println(DateTimeParser.parseDate("75.2"));
	}

	public static List<Pattern> getRegexes() {
		List<Pattern> regexes = new LinkedList<Pattern>();
		regexes.add(Pattern.compile(DateRegex1, Pattern.CASE_INSENSITIVE));
		regexes.add(Pattern.compile(DateRegex2, Pattern.CASE_INSENSITIVE));
		regexes.add(Pattern.compile(DateRegex3, Pattern.CASE_INSENSITIVE));
		regexes.add(Pattern.compile(DateRegex4, Pattern.CASE_INSENSITIVE));
		regexes.add(Pattern.compile(DateRegex5, Pattern.CASE_INSENSITIVE));
		regexes.add(Pattern.compile(DateRegex6, Pattern.CASE_INSENSITIVE));
		regexes.add(Pattern.compile(DateRegex7, Pattern.CASE_INSENSITIVE));
		regexes.add(Pattern.compile(DayMonthRegex1, Pattern.CASE_INSENSITIVE));
		regexes.add(Pattern.compile(DayMonthRegex2, Pattern.CASE_INSENSITIVE));
		regexes.add(Pattern.compile(MonthYearRegex, Pattern.CASE_INSENSITIVE));
		regexes.add(Pattern.compile(MonthYearRegex1, Pattern.CASE_INSENSITIVE));
		regexes.add(Pattern.compile(YearRegex, Pattern.CASE_INSENSITIVE));
		return regexes;

	}

	public static boolean parseDate(String date) {
		/*if (date.length() < 4) {
			if (NumericParser.parseNumeric(date))
				return false;
		}*/
		for (Pattern regexp : getRegexes()) {
		    if(regexp.matcher(date).matches()) {
                            return true;
                    }
		}
		return false;
	}
	
	

}
