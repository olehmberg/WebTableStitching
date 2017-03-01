package de.uni_mannheim.informatik.dws.t2k.utils.query;

public interface Func<TOut, TIn> {

    TOut invoke(TIn in);
    
}
