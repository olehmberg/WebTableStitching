package de.uni_mannheim.informatik.wdi.similarity.numeric;

import de.uni_mannheim.informatik.wdi.similarity.numeric.PercentageSimilarity;
import junit.framework.TestCase;

public class PercentageSimilarityTest extends TestCase {

	public void testCalculateDoubleDouble() {
		PercentageSimilarity sim = new PercentageSimilarity(0.33);
		
		assertEquals(0.394, Math.round(sim.calculate(2000.0, 2500.0) * 1000.0) / 1000.0);
		assertEquals(0.992, Math.round(sim.calculate(200000.0, 200500.0) * 1000.0) / 1000.0);
	}

}
