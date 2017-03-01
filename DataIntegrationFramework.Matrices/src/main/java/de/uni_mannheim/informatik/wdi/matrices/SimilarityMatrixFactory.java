package de.uni_mannheim.informatik.wdi.matrices;

/**
 * super class for all factory classes for similarity matrices
 * @author Oliver
 *
 */
public abstract class SimilarityMatrixFactory {
    public abstract <T> SimilarityMatrix<T> createSimilarityMatrix(int firstDimension, int secondDimension);
}
