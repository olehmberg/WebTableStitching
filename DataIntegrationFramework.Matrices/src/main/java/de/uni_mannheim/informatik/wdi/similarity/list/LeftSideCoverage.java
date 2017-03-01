package de.uni_mannheim.informatik.wdi.similarity.list;

import java.util.Collection;

import de.uni_mannheim.informatik.dws.t2k.utils.math.DoubleSet;
import de.uni_mannheim.informatik.wdi.matrices.SimilarityMatrix;
import de.uni_mannheim.informatik.wdi.matrices.matcher.BestChoiceMatching;

/**
 * The first set (left side) is important here. A similarity of 1 is reached if
 * each element of the first set has a corresponding element in the second set
 * (right side) with an inner similarity of 1
 * 
 * @author Oliver
 * 
 * @param <T>
 */
public class LeftSideCoverage<T extends Comparable<T>> extends
        ComplexSetSimilarity<T> {

	private static final long serialVersionUID = 1L;

	@Override
    protected Double aggregateSimilarity(SimilarityMatrix<T> matrix) {
        if(matrix.getFirstDimension().size()==0 || matrix.getSecondDimension().size()==0) {
            return 0.0;
        }
        
        BestChoiceMatching best = new BestChoiceMatching();
        best.setForceOneToOneMapping(true);
        SimilarityMatrix<T> bestMatrix = best.match(matrix);
        Collection<Double> scores = bestMatrix.getRowSums();

        // best only contains matched pairs, so we have to divide by the
        // dimension of the initial matrix to get the correct average
        return DoubleSet.sum(scores)
                / (double) matrix.getFirstDimension().size();
    }

}
