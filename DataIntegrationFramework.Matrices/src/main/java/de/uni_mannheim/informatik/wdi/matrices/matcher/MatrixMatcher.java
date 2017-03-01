package de.uni_mannheim.informatik.wdi.matrices.matcher;

import de.uni_mannheim.informatik.wdi.matrices.SimilarityMatrixFactory;
import de.uni_mannheim.informatik.wdi.matrices.SparseSimilarityMatrixFactory;

/**
 * super class for all second-line matchers (those that take similarity matrices as input and output another similarity matrix)
 * @author Oliver
 *
 */
public abstract class MatrixMatcher extends AbstractMatcher {

	SimilarityMatrixFactory similarityMatrixFactory;
	/**
	 * returns the similarity matrix factory that is used to create the similarity matrix containing the matching result
	 */
	public SimilarityMatrixFactory getSimilarityMatrixFactory() {
		return similarityMatrixFactory;
	}
	/**
	 * sets the similarity matrix factory that is used to create the similarity matrix containing the matching result
	 */
	public void setSimilarityMatrixFactory(SimilarityMatrixFactory factory) {
		similarityMatrixFactory = factory;
	}
	
	public MatrixMatcher()
	{
		setSimilarityMatrixFactory(new SparseSimilarityMatrixFactory());
	}
	
}
