package de.uni_mannheim.informatik.wdi.matrices.matcher;

/**
 * Represents a pair of instances that were compared during matching (only used for logging)
 * @author Oliver
 *
 * @param <T>
 */
public class MatchingPair<T> {
    private T first;
    private T second;
    
    public T getFirst() {
        return first;
    }
    public void setFirst(T first) {
        this.first = first;
    }
    public T getSecond() {
        return second;
    }
    public void setSecond(T second) {
        this.second = second;
    }
    
    public MatchingPair(T first, T second) {
        setFirst(first);
        setSecond(second);
    }
}
