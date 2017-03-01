package de.uni_mannheim.informatik.dws.t2k.utils.data.string;

import java.util.regex.Pattern;

import org.apache.commons.lang3.StringEscapeUtils;

public class StringCleaner {
    
    private static final Pattern removePattern = Pattern.compile("\"|\\||,|\\{|\\}|<.*>");
    private static final Pattern whitespacePattern = Pattern.compile("\n|\\s+|&*nbsp;*");
    private static final Pattern bracketsPattern = Pattern.compile("\\(.*\\)");
    
    public static String cleanString(String value,
            boolean removeContentInBrackets) {
        try {
            value = StringEscapeUtils.unescapeJava(value);
            //value = value.replace("\"", "");
            //value = value.replace("|", " ");
            //value = value.replace(",", "");
            //value = value.replace("{", "");
            //value = value.replace("}", "");
            value = removePattern.matcher(value).replaceAll("");
            
            //value = value.replaceAll("\n", " ");
            //value = value.replaceAll("\\s+", " ");
            //value = value.replace("&nbsp;", " ");
            //value = value.replace("&nbsp", " ");
            //value = value.replace("nbsp", " ");
            value = whitespacePattern.matcher(value).replaceAll(" ");
            
            //value = value.replaceAll("<.*>", "");
            if (removeContentInBrackets) {
                //value = value.replaceAll("\\(.*\\)", "");
                value = bracketsPattern.matcher(value).replaceAll("");
            }
            if (value.equals("")) {
                value = null;
            } else {
                value = value.toLowerCase();
                value = value.trim();
            }
        } catch (Exception e) {
        }
        return value;
    }
}
