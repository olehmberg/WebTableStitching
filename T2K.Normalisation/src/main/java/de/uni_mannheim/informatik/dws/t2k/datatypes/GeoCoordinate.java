/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.uni_mannheim.informatik.dws.t2k.datatypes;

/**
 *
 * @author domi
 */
public class GeoCoordinate {
    
    private Double latitude;
    private Double longitude;
    private Double oneValue;
    
    public GeoCoordinate(){}
    
    public static GeoCoordinate parseCoordinate(String cooridante) {
        Double longi=Double.MAX_VALUE, lat=Double.MAX_VALUE;
        try {
        if(!cooridante.contains(",") && !cooridante.contains("\\s")) {
            GeoCoordinate g = new GeoCoordinate();
            g.oneValue = Double.parseDouble(cooridante);
        }     
        if(cooridante.contains(",")) {
            lat = Double.parseDouble(cooridante.split(" ")[0]);
            longi = Double.parseDouble(cooridante.split(" ")[1]);
        }
        else {
            String[] values = cooridante.split("\\s");
            
            if(values.length>1) {
                lat = Double.parseDouble(cooridante.split("\\s")[0]);
                longi = Double.parseDouble(cooridante.split("\\s")[1]);
            }
        }
        } catch(Exception e ) {
            //e.printStackTrace();
        }
        GeoCoordinate g = new GeoCoordinate();
        g.latitude = lat;
        g.longitude = longi;
        return g;
    }
}
