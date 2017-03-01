/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.uni_mannheim.informatik.dws.t2k.units;

import java.util.Collection;

/**
 *
 * @author domi
 */
public class Unit {
    
    private String name;
    private Collection<String> abbreviations;
    private double factor;

    /**
     * @return the name
     */
    public String getName() {
        return name;
    }

    /**
     * @param name the name to set
     */
    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return the abbreviations
     */
    public Collection<String> getAbbreviations() {
        return abbreviations;
    }

    /**
     * @param abbreviations the abbreviations to set
     */
    public void setAbbreviations(Collection<String> abbreviations) {
        this.abbreviations = abbreviations;
    }

    /**
     * @return the factor
     */
    public double getFactor() {
        return factor;
    }

    /**
     * @param factor the factor to set
     */
    public void setFactor(double factor) {
        this.factor = factor;
    }
    
}
