package de.uni_mannheim.informatik.wdi.matrices.matcher;

import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;

import de.uni_mannheim.informatik.dws.t2k.utils.concurrent.Consumer;
import de.uni_mannheim.informatik.dws.t2k.utils.concurrent.Parallel;
import de.uni_mannheim.informatik.wdi.matrices.SimilarityMatrix;

/**
 * keeps only the top K similarity values for each instance (of the first dimension) in the resulting similarity matrix
 * @author Oliver
 *
 */
public class TopKCandidates extends MatrixMatcher {

    public <T extends Comparable<T>> SimilarityMatrix<T> match(final SimilarityMatrix<T> similarities, final int k) {
        
        final SimilarityMatrix<T> sim = getSimilarityMatrixFactory().createSimilarityMatrix(similarities.getFirstDimension().size(), similarities.getSecondDimension().size());

        try {
            new Parallel<T>(isRunInParallel() ? 0 : 1).foreach(similarities.getFirstDimension(), new Consumer<T>() {

                @Override
                public void execute(T parameter) {
                    setTopKForInstance(parameter, similarities, sim, k);
                }
            }, "TopKCandidates");
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        return sim;
    }
    
    protected <T extends Comparable<T>> void setTopKForInstance(final T instance, final SimilarityMatrix<T> similarities, SimilarityMatrix<T> sim, int k) {
        // we need the treeset first to put the elements in a stable natural ordering, otherwise the sort is unpredictable for elements with the same score
        //TreeSet<T> matches = new TreeSet<T>();
        //matches.addAll(similarities.getMatches(instance));
        
        // now put the (pre-) ordered elements in a list that we can sort by score
        LinkedList<T> top = new LinkedList<T>();
        //top.addAll(matches);
        top.addAll(similarities.getMatches(instance));
        
        // sort the list
        Collections.sort(top, new Comparator<T>() {

            public int compare(T o1, T o2) {
                int i = -Double.compare(similarities.get(instance, o1), similarities.get(instance, o2));
                
                if(i==0) {
                    return Integer.compare(o1.hashCode(), o2.hashCode());
                } else {
                    return i;
                }
                //return -Double.compare(similarities.get(instance, o1), similarities.get(instance, o2));
            }
        });
        
        k = Math.min(k, top.size());
        
        // and take the top k elements
        synchronized (sim) {
            for(int i = 0; i < k; i++) {
                sim.set(instance, top.get(i), similarities.get(instance, top.get(i)));
            }
        }
    }
    
}
