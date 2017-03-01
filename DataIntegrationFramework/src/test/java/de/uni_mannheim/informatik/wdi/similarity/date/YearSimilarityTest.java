package de.uni_mannheim.informatik.wdi.similarity.date;

import org.joda.time.DateTime;

import de.uni_mannheim.informatik.wdi.similarity.date.YearSimilarity;
import junit.framework.TestCase;

public class YearSimilarityTest extends TestCase {

	public void testCalculateDateTimeDateTime() {
		YearSimilarity sim = new YearSimilarity(10);
		
		DateTime dt1 = DateTime.parse("2015-01-01");
		DateTime dt2 = DateTime.parse("2014-01-01");
		DateTime dt3 = DateTime.parse("2010-01-01");
		DateTime dt4 = DateTime.parse("2005-01-01");
		DateTime dt5 = DateTime.parse("1905-01-01");
		
		assertEquals(1.0, sim.calculate(dt1, dt1));
		assertEquals(0.9, sim.calculate(dt1, dt2));
		assertEquals(0.9, sim.calculate(dt2, dt1));
		assertEquals(0.5, sim.calculate(dt1, dt3));
		assertEquals(0.0, sim.calculate(dt1, dt4));
		assertEquals(0.0, sim.calculate(dt1, dt5));
	}

}
