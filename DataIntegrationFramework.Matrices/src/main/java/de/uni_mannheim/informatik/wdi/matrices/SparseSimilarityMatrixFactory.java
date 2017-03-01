package de.uni_mannheim.informatik.wdi.matrices;

/**
 * factory class for SparseSimilarityMatrix
 * @author Oliver
 *
 */
public class SparseSimilarityMatrixFactory extends SimilarityMatrixFactory {

	public <T> SimilarityMatrix<T> createSimilarityMatrix(int firstDimension, int secondDimension) {
		return new SparseSimilarityMatrix<T>(firstDimension, secondDimension);
	    //return new FastSparseSimilarityMatrix<>(firstDimension, secondDimension);
	}

}
