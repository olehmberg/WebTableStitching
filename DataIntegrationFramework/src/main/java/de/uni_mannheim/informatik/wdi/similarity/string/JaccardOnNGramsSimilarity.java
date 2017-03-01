package de.uni_mannheim.informatik.wdi.similarity.string;

import com.wcohen.ss.Jaccard;
import com.wcohen.ss.tokens.NGramTokenizer;
import com.wcohen.ss.tokens.SimpleTokenizer;
import de.uni_mannheim.informatik.wdi.similarity.SimilarityMeasure;

public class JaccardOnNGramsSimilarity extends SimilarityMeasure<String> {

	private static final long serialVersionUID = 1L;
	private int gramSize = 3;
    
    public JaccardOnNGramsSimilarity(int n) {
        gramSize = n;
    }
    
    @Override
    public double calculate(String first, String second) {
        if(first == null || second == null) {
            return 0.0;
        }
        
        NGramTokenizer tok = new NGramTokenizer(gramSize, gramSize, false, SimpleTokenizer.DEFAULT_TOKENIZER);
        Jaccard j = new Jaccard(tok);
        return j.score(first, second);
    }

}
