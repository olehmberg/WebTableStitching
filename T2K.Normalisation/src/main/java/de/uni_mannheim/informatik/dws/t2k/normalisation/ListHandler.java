package de.uni_mannheim.informatik.dws.t2k.normalisation;

import java.util.List;
import java.util.regex.Pattern;

public class ListHandler {

//    private static Pattern listPattern = Pattern.compile("^\\{.+\\|.*\\}$");
	private static Pattern listPattern = Pattern.compile("^\\{.+\\}$");
    
    public static boolean checkIfList(String columnValue) {
        if(columnValue!=null && listPattern.matcher(columnValue).matches()) {
            return true;
        }
        return false;
    }
    
    public static String[] splitList(String columnValue) {
//        String data = columnValue.replace("{", "").replace("}", "");
//        return data.split("\\|");
        String data = columnValue.substring(1, columnValue.length() - 1);
        return data.split("\\|");
    }
    
    public static String formatList(List<String> values) {
        StringBuilder sb = new StringBuilder();
        
        sb.append("{");
        
        for(int i = 0; i < values.size(); i++) {
            if(values.get(i)!=null) {
	            if(i!=0) {
	                sb.append("|");
	            }
	            
	            sb.append(values.get(i).replace("|", ""));
            }
        }
        
        sb.append("}");
        
        return sb.toString();
    }
}
