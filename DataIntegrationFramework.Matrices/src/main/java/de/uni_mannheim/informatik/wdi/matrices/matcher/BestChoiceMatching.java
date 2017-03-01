package de.uni_mannheim.informatik.wdi.matrices.matcher;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import de.uni_mannheim.informatik.wdi.matrices.SimilarityMatrix;

/**
 * Greedy approach that selects the best stable match for each instance of the first dimension
 * @author Oliver
 *
 */
public class BestChoiceMatching extends MatrixMatcher {

    private boolean forceOneToOneMapping = true;
    public boolean isForceOneToOneMapping() {
        return forceOneToOneMapping;
    }
    public void setForceOneToOneMapping(boolean forceOneToOneMapping) {
        this.forceOneToOneMapping = forceOneToOneMapping;
    }
    
    public <T extends Comparable<T>> SimilarityMatrix<T> match(SimilarityMatrix<T> input) {
        
        SimilarityMatrix<T> sim = getSimilarityMatrixFactory().createSimilarityMatrix(input.getFirstDimension().size(), input.getSecondDimension().size());
        
        Set<T> alreadyMatched = new HashSet<T>();
        
        // order all items so that we get consistent results in cases where the score is equal 
        ArrayList<T>  dimension = new ArrayList<>(input.getFirstDimension());
        Collections.sort(dimension);
        for(T instance : dimension) {
            
            double max = 0.0;
            T best = null;
            ArrayList<T> matches = new ArrayList<>(input.getMatches(instance));
            Collections.sort(matches);
            // determine best match
            for(T candidate : matches) {
                
                if(!alreadyMatched.contains(candidate) && input.get(instance, candidate)>max) {
                    max = input.get(instance, candidate);
                    best = candidate;
                }
                
            }
            
            // make sure instance is also the best match for candidate (i.e. is this a stable pair)
            for(T instance2 : input.getFirstDimension()) {
                
                if(instance2!=instance && !alreadyMatched.contains(instance2) && input.get(instance2, best)!=null && input.get(instance2, best)>max) {
                    best = null;
                    break;
                }
                
            }
            
            // if we found a stable pair
            if(best!=null) {
                sim.set(instance, best, max);
                
                if(isForceOneToOneMapping()) {
                    alreadyMatched.add(instance);
                    alreadyMatched.add(best);
                }
            }
        }
        
        return sim;
    }

}
