package de.uni_mannheim.informatik.wdi.similarity.list;

import de.uni_mannheim.informatik.wdi.matrices.SimilarityMatrix;
import de.uni_mannheim.informatik.wdi.matrices.matcher.BestChoiceMatching;

public class GeneralisedJaccard<T extends Comparable<T>> extends ComplexSetSimilarity<T> {

	private static final long serialVersionUID = 1L;

	@Override
	protected Double aggregateSimilarity(SimilarityMatrix<T> matrix) {
        double firstLength = matrix.getFirstDimension().size();
        double secondLength = matrix.getSecondDimension().size();
        
        BestChoiceMatching best = new BestChoiceMatching();
        best.setForceOneToOneMapping(true);
        matrix = best.match(matrix);
        
        double fuzzyMatching = matrix.getSum();
        
        return fuzzyMatching / (firstLength + secondLength - fuzzyMatching);
	}

}
