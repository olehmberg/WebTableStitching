package de.uni_mannheim.informatik.wdi.matrices.matcher;

import java.util.TreeSet;

import de.uni_mannheim.informatik.wdi.matrices.SimilarityMatrix;

/**
 * Enforces a 1:1 mapping between the first and second dimension of a similarity matrix
 * @author Oliver
 *
 * @param <T>
 */
public class OneToOneConstraint extends MatrixMatcher {
	
	private ConflictResolution conflictResolution;
	public ConflictResolution getConflictResolution() {
		return conflictResolution;
	}
	public void setConflictResolution(ConflictResolution conflictResolution) {
		this.conflictResolution = conflictResolution;
	}
	
	public OneToOneConstraint(ConflictResolution conflictResolution)
	{
		setConflictResolution(conflictResolution);
	}
	
	public <T extends Comparable<T>> SimilarityMatrix<T> match(SimilarityMatrix<T> input)
	{
		SimilarityMatrix<T> sim = getSimilarityMatrixFactory().createSimilarityMatrix(input.getFirstDimension().size(), input.getSecondDimension().size());
		
        // order all items so that we get consistent results in cases where the score is equal 
        TreeSet<T> dimension = new TreeSet<T>();
        dimension.addAll(input.getFirstDimension());
        
		for(T instance : dimension) {
			
			Double currentScore = null;
			
			T currentItem = null;
			
			TreeSet<T> matches = new TreeSet<T>();
			
			try {
			    matches.addAll(input.getMatches(instance));
            } catch (Exception e) {
                System.out.println(String.format("Exception for instance %s (%s)", instance, input.getLabel(instance)));
                System.out.println(String.format("%d matches:", input.getMatches(instance).size()));
                for(T cand : input.getMatches(instance)) {
                    System.out.println(String.format("\t%s (%s)", cand, input.getLabel(cand)));
                }
                
                //throw e;
                // as we cannot re-throw this exception without changing the class definition, we just repeat what caused the exception ... 
                matches.addAll(input.getMatches(instance));
            }
			
			for(T candidate : matches) {
				Double score = input.get(instance, candidate);
                                
                                if(isBetter(score, currentScore))
				{
					currentScore = score;
					currentItem = candidate;
				}
				
			}
			
			if(currentScore!=null) {
			    sim.set(instance, currentItem, currentScore);
			}
			
		}
		
		return sim;
	}
	
	protected boolean isBetter(Double score, Double currentScore) {
	    if(score==null) {
	        return false;
	    } else if(currentScore==null) {
	        return true;
	    }
	    
		switch (getConflictResolution()) {
		case Maximum:
			return score >= currentScore;
		case Minimum:
			return score <= currentScore;
		default:
			return false;
		}
	}
	
}
