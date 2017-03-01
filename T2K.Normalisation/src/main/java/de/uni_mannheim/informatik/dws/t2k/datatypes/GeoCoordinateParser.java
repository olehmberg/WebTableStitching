package de.uni_mannheim.informatik.dws.t2k.datatypes;

import java.util.regex.Pattern;

public class GeoCoordinateParser {

    /**
     * this regex is more complex and catches all types of coordinates, but
     * often catches single double numbers
     */
    public static final String GeoCoordRegex = "([SNsn][\\s]*)?((?:[\\+-]?[0-9]*[\\.,][0-9]+)|(?:[\\+-]?[0-9]+))(?:(?:[^ms'′\"″,\\.\\dNEWnew]?)|(?:[^ms'′\"″,\\.\\dNEWnew]+((?:[\\+-]?[0-9]*[\\.,][0-9]+)|(?:[\\+-]?[0-9]+))(?:(?:[^ds°\"″,\\.\\dNEWnew]?)|(?:[^ds°\"″,\\.\\dNEWnew]+((?:[\\+-]?[0-9]*[\\.,][0-9]+)|(?:[\\+-]?[0-9]+))[^dm°'′,\\.\\dNEWnew]*))))([SNsn]?)[^\\dSNsnEWew]+([EWew][\\s]*)?((?:[\\+-]?[0-9]*[\\.,][0-9]+)|(?:[\\+-]?[0-9]+))(?:(?:[^ms'′\"″,\\.\\dNEWnew]?)|(?:[^ms'′\"″,\\.\\dNEWnew]+((?:[\\+-]?[0-9]*[\\.,][0-9]+)|(?:[\\+-]?[0-9]+))(?:(?:[^ds°\"″,\\.\\dNEWnew]?)|(?:[^ds°\"″,\\.\\dNEWnew]+((?:[\\+-]?[0-9]*[\\.,][0-9]+)|(?:[\\+-]?[0-9]+))[^dm°'′,\\.\\dNEWnew]*))))([EWew]?)";
    public static final Pattern GEO_COORD_REGEX_SIMPLE = Pattern.compile("^([-+]?\\d{1,2}([.]\\d+)?),?\\s+([-+]?\\d{1,3}([.]\\d+)?)$", Pattern.CASE_INSENSITIVE);
    public static final Pattern GEO_COORD_REGEX_SIMPLE1 = Pattern.compile("^[-+]?\\d{1,2}[.]\\d{4,8}$", Pattern.CASE_INSENSITIVE);

    public static boolean parseGeoCoordinate(String text) {
        if (GEO_COORD_REGEX_SIMPLE.matcher(text).matches()) {
            return true;
        }
        if (GEO_COORD_REGEX_SIMPLE1.matcher(text).matches()) {
            if(Double.parseDouble(text)>-180.00 && Double.parseDouble(text)<180.00 ) {
                return true;
            }
        }
        return false;
    }

    public static void main(String[] args) {
        System.out.println(parseGeoCoordinate("41.1775 20.6788"));
        System.out.println(parseGeoCoordinate("50.83924"));
        System.out.println(parseGeoCoordinate("49.62297"));        
    }
}
