/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package de.uni_mannheim.informatik.wdi.similarity.numeric;

import de.uni_mannheim.informatik.wdi.similarity.SimilarityMeasure;



/**
 *
 * @author domi
 */
public class DeviationSimilarity extends SimilarityMeasure<Double> {

	private static final long serialVersionUID = 1L;

	@Override
    public double calculate(Double first, Double second) {                
        if(first==null || second == null) {
            return 0.0;
        }
        if(first.equals(second)) {
            return 1.0;
        }
        else {
            return 0.5*Math.min(Math.abs(first),Math.abs(second))/Math.max(Math.abs(first),Math.abs(second));
        }
    }    
}
