/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.uni_mannheim.informatik.dws.t2k.utils.data.link;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;

/**
 *
 * @author domi
 */
public class DBpediaURIEncoder {

    public static String encodeURIForDBpedia(String uri) throws UnsupportedEncodingException {
        String fragment ="";
        if(uri.contains("/resource/")) {
            fragment = uri.split("/resource/")[1];
        }
        if(uri.contains("/page/")) {
            fragment = uri.split("/page/")[1];
        }        
        fragment = URLEncoder.encode(fragment, "UTF-8");
        if (fragment.contains("%25")) {            
            fragment = fragment.replace("%25", "%");
        }
        if (fragment.contains("%2C")) {
            fragment = fragment.replace("%2C", ",");
        }
        if (fragment.contains("%27")) {
            fragment = fragment.replace("%27", "'");
        }
        if (fragment.contains("%28")) {
            fragment = fragment.replace("%28", "(");
        }
        if (fragment.contains("%29")) {
            fragment = fragment.replace("%29", ")");
        }
        if (fragment.contains("%3A")) {
            fragment = fragment.replace("%3A", ":");
        }        
        //String key = s[1].replaceAll("\\s+", " ");
        String encodedURI = "http://dbpedia.org/resource/" + fragment;
        return encodedURI;
    }
}
