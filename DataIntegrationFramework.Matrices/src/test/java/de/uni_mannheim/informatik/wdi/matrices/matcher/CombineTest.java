package de.uni_mannheim.informatik.wdi.matrices.matcher;

import de.uni_mannheim.informatik.wdi.matrices.ArrayBasedSimilarityMatrixFactory;
import de.uni_mannheim.informatik.wdi.matrices.SimilarityMatrix;
import de.uni_mannheim.informatik.wdi.matrices.matcher.CombinationType;
import de.uni_mannheim.informatik.wdi.matrices.matcher.Combine;
import de.uni_mannheim.informatik.wdi.test.TestInstance;
import junit.framework.TestCase;

public class CombineTest extends TestCase {

    public void testSum() {
        Combine<TestInstance> c = new Combine<TestInstance>();
        
        SimilarityMatrix<TestInstance> first = new ArrayBasedSimilarityMatrixFactory().createSimilarityMatrix(2, 2);
        TestInstance inst1 = new TestInstance(1, "aa");
        TestInstance inst2 = new TestInstance(2, "ab");
        TestInstance cand1 = new TestInstance(11, "aa");
        TestInstance cand2 = new TestInstance(22, "ab");
        
        first.set(inst1, cand1, 1.0);
        first.set(inst1, cand2, 2.0);
        first.set(inst2, cand1, 1.0);
        first.set(inst2, cand2, 2.0);
        
        SimilarityMatrix<TestInstance> second = new ArrayBasedSimilarityMatrixFactory().createSimilarityMatrix(2, 2);
        second.set(inst1, cand1, 3.0);
        second.set(inst1, cand2, 4.0);
        second.set(inst2, cand1, 3.0);
        second.set(inst2, cand2, 4.0);
        
        c.setAggregationType(CombinationType.Sum);
        SimilarityMatrix<TestInstance> multiplied = c.match(first, second);
        assertEquals(4.0, multiplied.get(inst1, cand1));
        assertEquals(6.0, multiplied.get(inst1, cand2));
        assertEquals(4.0, multiplied.get(inst2, cand1));
        assertEquals(6.0, multiplied.get(inst2, cand2));
    }
    
    public void testAvg() {
        Combine<TestInstance> c = new Combine<TestInstance>();
        
        SimilarityMatrix<TestInstance> first = new ArrayBasedSimilarityMatrixFactory().createSimilarityMatrix(2, 2);
        TestInstance inst1 = new TestInstance(1, "aa");
        TestInstance inst2 = new TestInstance(2, "ab");
        TestInstance cand1 = new TestInstance(11, "aa");
        TestInstance cand2 = new TestInstance(22, "ab");
        
        first.set(inst1, cand1, 1.0);
        first.set(inst1, cand2, 2.0);
        first.set(inst2, cand1, 1.0);
        first.set(inst2, cand2, 2.0);
        
        SimilarityMatrix<TestInstance> second = new ArrayBasedSimilarityMatrixFactory().createSimilarityMatrix(2, 2);
        second.set(inst1, cand1, 3.0);
        second.set(inst1, cand2, 4.0);
        second.set(inst2, cand1, 3.0);
        second.set(inst2, cand2, 4.0);
        
        c.setAggregationType(CombinationType.Average);
        SimilarityMatrix<TestInstance> multiplied = c.match(first, second);
        assertEquals(2.0, multiplied.get(inst1, cand1));
        assertEquals(3.0, multiplied.get(inst1, cand2));
        assertEquals(2.0, multiplied.get(inst2, cand1));
        assertEquals(3.0, multiplied.get(inst2, cand2));
    }
    
    public void testWeightedSum() {
        Combine<TestInstance> c = new Combine<TestInstance>();
        
        SimilarityMatrix<TestInstance> first = new ArrayBasedSimilarityMatrixFactory().createSimilarityMatrix(2, 2);
        TestInstance inst1 = new TestInstance(1, "aa");
        TestInstance inst2 = new TestInstance(2, "ab");
        TestInstance cand1 = new TestInstance(11, "aa");
        TestInstance cand2 = new TestInstance(22, "ab");
        
        first.set(inst1, cand1, 1.0);
        first.set(inst1, cand2, 2.0);
        first.set(inst2, cand1, 1.0);
        first.set(inst2, cand2, 2.0);
        
        SimilarityMatrix<TestInstance> second = new ArrayBasedSimilarityMatrixFactory().createSimilarityMatrix(2, 2);
        second.set(inst1, cand1, 3.0);
        second.set(inst1, cand2, 4.0);
        second.set(inst2, cand1, 3.0);
        second.set(inst2, cand2, 4.0);
        
        c.setAggregationType(CombinationType.WeightedSum);
        c.setFirstWeight(2.0);
        c.setSecondWeight(0.5);
        SimilarityMatrix<TestInstance> multiplied = c.match(first, second);
        assertEquals(3.5, multiplied.get(inst1, cand1));
        assertEquals(6.0, multiplied.get(inst1, cand2));
        assertEquals(3.5, multiplied.get(inst2, cand1));
        assertEquals(6.0, multiplied.get(inst2, cand2));
    }
    
    public void testMultiply() {
        Combine<TestInstance> c = new Combine<TestInstance>();
        
        SimilarityMatrix<TestInstance> first = new ArrayBasedSimilarityMatrixFactory().createSimilarityMatrix(2, 2);
        TestInstance inst1 = new TestInstance(1, "aa");
        TestInstance inst2 = new TestInstance(2, "ab");
        TestInstance cand1 = new TestInstance(11, "aa");
        TestInstance cand2 = new TestInstance(22, "ab");
        
        first.set(inst1, cand1, 1.0);
        first.set(inst1, cand2, 2.0);
        first.set(inst2, cand1, 1.0);
        first.set(inst2, cand2, 2.0);
        
        SimilarityMatrix<TestInstance> second = new ArrayBasedSimilarityMatrixFactory().createSimilarityMatrix(2, 2);
        second.set(inst1, cand1, 3.0);
        second.set(inst1, cand2, 4.0);
        second.set(inst2, cand1, 3.0);
        second.set(inst2, cand2, 4.0);
        
        c.setAggregationType(CombinationType.Multiply);
        SimilarityMatrix<TestInstance> multiplied = c.match(first, second);
        assertEquals(3.0, multiplied.get(inst1, cand1));
        assertEquals(8.0, multiplied.get(inst1, cand2));
        assertEquals(3.0, multiplied.get(inst2, cand1));
        assertEquals(8.0, multiplied.get(inst2, cand2));
        
    }
}
