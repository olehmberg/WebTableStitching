package de.uni_mannheim.informatik.wdi.similarity.list;

import de.uni_mannheim.informatik.wdi.matrices.SimilarityMatrix;

/**
 * Set-based similarity function that returns the maximum similarity between any two elements of both sets
 * @author Oliver
 *
 * @param <T>
 */
public class MaxSimilarity<T> extends ComplexSetSimilarity<T> {

	private static final long serialVersionUID = 1L;

	@Override
    protected Double aggregateSimilarity(SimilarityMatrix<T> matrix) {
        return matrix.getMaxValue();
    }

}
