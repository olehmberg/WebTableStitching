package de.uni_mannheim.informatik.wdi.similarity.numeric;

import de.uni_mannheim.informatik.wdi.similarity.numeric.AbsoluteDifferenceSimilarity;
import junit.framework.TestCase;

public class AbsoluteDifferenceSimilarityTest extends TestCase {

	public void testCalculateDoubleDouble() {
		AbsoluteDifferenceSimilarity sim = new AbsoluteDifferenceSimilarity(1000);
		
		assertEquals(0.5, sim.calculate(2000.0, 2500.0));
		assertEquals(0.5, sim.calculate(200000.0, 200500.0));
	}

}
