package de.uni_mannheim.informatik.wdi.similarity.string;

import de.uni_mannheim.informatik.wdi.similarity.string.GeneralisedStringJaccard;
import de.uni_mannheim.informatik.wdi.similarity.string.LevenshteinSimilarity;
import junit.framework.TestCase;

public class GeneralisedStringJaccardTest extends TestCase {

    public void testCalculate() {
        
        GeneralisedStringJaccard j = new GeneralisedStringJaccard(new LevenshteinSimilarity(), 0.0, 0.0);
        String s1, s2;
        
        s1 = "aa cc";
        s2 = "aa bb";
        
        assertEquals(33, (int)(j.calculate(s1, s2)*100));
        
        s1 = "nba mcgrady";
        s2 = "macgrady nba";
        
        assertEquals(88, (int)(j.calculate(s1, s2)*100));
        
        s1 = "nba wnba mcgrady";
        s2 = "macgrady nba";
        
        assertEquals(60, (int)(j.calculate(s1, s2)*100));
        
        j = new GeneralisedStringJaccard(new LevenshteinSimilarity(), 0.5, 0.5);
        
        s1 = "nba wnba mcgrady";
        s2 = "macgrady nba";
        
        assertEquals(60, (int)(j.calculate(s1, s2)*100));
        
        s1 = "democratic";
        s2 = "Democratic Party ";
        		
        assertEquals(50, (int)(j.calculate(s1, s2)*100));

    }
    
}
