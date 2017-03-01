package de.uni_mannheim.informatik.wdi.similarity.string;

import java.util.LinkedList;
import java.util.List;

import com.wcohen.ss.api.Token;
import com.wcohen.ss.tokens.SimpleTokenizer;

import de.uni_mannheim.informatik.wdi.similarity.SimilarityMeasure;
import de.uni_mannheim.informatik.wdi.similarity.list.GeneralisedJaccard;

public class GeneralisedStringJaccard extends SimilarityMeasure<String>  {

	private static final long serialVersionUID = 1L;
	private SimilarityMeasure<String> innerFunction;
    public SimilarityMeasure<String> getInnerFunction() {
        return innerFunction;
    }
    public void setInnerFunction(SimilarityMeasure<String> innerFunction) {
        this.innerFunction = innerFunction;
    }
    
    private double innerThreshold;
    public double getInnerThreshold() {
        return innerThreshold;
    }
    public void setInnerThreshold(double innerThreshold) {
        this.innerThreshold = innerThreshold;
    }
    
    private double JaccardThreshold;
    public double getJaccardThreshold() {
        return JaccardThreshold;
    }
    public void setJaccardThreshold(double jaccardThreshold) {
        JaccardThreshold = jaccardThreshold;
    }
    
    public GeneralisedStringJaccard(SimilarityMeasure<String> innerSimilarityFunction, double innerSimilarityThreshold, double jaccardThreshold) {
        setInnerFunction(innerSimilarityFunction);
        setInnerThreshold(innerSimilarityThreshold);
        setJaccardThreshold(jaccardThreshold);
    }
    
    @Override
    public double calculate(String first, String second) {
        
        // split strings into tokens
        SimpleTokenizer tok = new SimpleTokenizer(true, true);
        
        List<String> f = new LinkedList<>();
        List<String> s = new LinkedList<>();
        
        if(first!=null) {
            for(Token t : tok.tokenize(first)) {
                f.add(t.getValue());
            }
        }
        
        if(second!=null) {
            for(Token t : tok.tokenize(second)) {
                s.add(t.getValue());
            }
        }
        
        // run Set-based similarity function
        GeneralisedJaccard<String> j = new GeneralisedJaccard<>();        
        j.setInnerSimilarityThreshold(getInnerThreshold());
        j.setInnerSimilarity(getInnerFunction());
        double sim = j.calculate(f, s);
        
        return sim >= getJaccardThreshold() ? sim : 0.0;
    }

}
