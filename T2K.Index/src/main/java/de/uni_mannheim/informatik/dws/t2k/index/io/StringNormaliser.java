package de.uni_mannheim.informatik.dws.t2k.index.io;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.core.LowerCaseFilter;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.core.WhitespaceTokenizer;
import org.apache.lucene.analysis.en.PorterStemFilter;
import org.apache.lucene.analysis.miscellaneous.WordDelimiterFilterFactory;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.util.StopwordAnalyzerBase;
import org.apache.lucene.util.Version;

public class StringNormaliser {

    /**
     * splits the string into tokens and concatenates it again, inserting white spaces between all tokens
     * @param s
     * @return
     */
    public static String normalise(String s, boolean useStemmer) {
        
        return StringUtils.join(tokenise(s, useStemmer), " ");
        
    }
    
    public static List<String> tokenise(String s, boolean useStemmer) {
        Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_46);
        List<String> result = new ArrayList<String>();

        try {

            Map<String, String> args = new HashMap<String, String>();
            args.put("generateWordParts", "1");
            args.put("generateNumberParts", "1");
            args.put("catenateNumbers", "0");
            args.put("splitOnCaseChange", "1");
            WordDelimiterFilterFactory fact = new WordDelimiterFilterFactory(args);

            // resolve non unicode chars
            s = StringEscapeUtils.unescapeJava(s);
            
            // remove brackets (but keep content)
                s = s.replaceAll("[\\(\\)]", "");
                
            // tokenise
            TokenStream stream = fact.create(new WhitespaceTokenizer(
                    Version.LUCENE_46, new StringReader(s)));
            stream.reset();

            if(useStemmer) {
                // use stemmer if requested
                stream = new PorterStemFilter(stream);
            }
            
            // lower case all tokens
            stream = new LowerCaseFilter(Version.LUCENE_46, stream);
            
            // remove stop words
            stream = new StopFilter(Version.LUCENE_46, stream,
                    ((StopwordAnalyzerBase) analyzer).getStopwordSet());
            
            // enumerate tokens
            while (stream.incrementToken()) {
                result.add(stream.getAttribute(CharTermAttribute.class)
                        .toString());
            }
            stream.close();
        } catch (IOException e) {
            // not thrown b/c we're using a string reader...
        }
        
        analyzer.close();
        
        return result;
    }
}
