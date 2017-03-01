package de.uni_mannheim.informatik.wdi.matrices;

/**
 * Factory class for ArrayBasedSimilarityMatrix
 * @author Oliver
 *
 */
public class ArrayBasedSimilarityMatrixFactory extends SimilarityMatrixFactory {

	public <T> SimilarityMatrix<T> createSimilarityMatrix(int firstDimension,
			int secondDimension) {
		return new ArrayBasedSimilarityMatrix<T>(firstDimension, secondDimension);
	}

}
