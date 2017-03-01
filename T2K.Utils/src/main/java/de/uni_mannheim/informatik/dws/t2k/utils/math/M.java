package de.uni_mannheim.informatik.dws.t2k.utils.math;

public class M {

    public static <T extends Comparable<T>> T min(T first, T second) {
        if(first==null) {
            return second;
        }
        
        if(second==null) {
            return first;
        }
        
        if(first.compareTo(second)<0) {
            return first;
        } else {
            return second;
        }
    }
    
    public static <T extends Comparable<T>> T max(T first, T second) {
        
        if(first==null) {
            return second;
        }
        
        if(second==null) {
            return first;
        }
        
        if(first.compareTo(second)>0) {
            return first;
        } else {
            return second;
        }
    }
    
}
