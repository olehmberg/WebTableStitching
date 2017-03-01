/**
 * 
 */
package de.uni_mannheim.informatik.dws.t2k.match.data;

import java.util.HashMap;
import java.util.HashSet;

import junit.framework.TestCase;

/**
 * @author Sanikumar
 *
 */
public class SurfaceFormsTest extends TestCase {
	/**
	 * Test method for {@link de.uni_mannheim.informatik.dws.t2k.match.data.SurfaceForms#loadSurfaceForms()}
	 */
	
	public void testLoadSurfaceForms(){
		HashMap<String, HashSet<String>> surfaceForms = new HashMap<>();
		
		HashSet<String> hs1 = new HashSet<>();
		hs1.add("Deutschland");
		hs1.add("Germany");
		surfaceForms.put("Germany", hs1);
		
		HashSet<String> hs2 = new HashSet<>();
		hs2.add("Hindustan");
		hs2.add("Bharat");
		surfaceForms.put("India", hs2);
		
		SurfaceForms sf = new SurfaceForms(surfaceForms);
		
//		checks whether we get correct surface forms for given entity?
		assertEquals(hs1, sf.getSurfaceForms("Germany"));
		assertEquals(hs2, sf.getSurfaceForms("India"));

//		check for null pointer exception if no surface forms were found for given entity
		assertNotNull(sf.getSurfaceForms("abc"));
	}
}
