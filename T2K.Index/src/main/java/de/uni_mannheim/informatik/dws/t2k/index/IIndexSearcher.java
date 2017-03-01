package de.uni_mannheim.informatik.dws.t2k.index;

import java.util.Collection;
import java.util.List;

public interface IIndexSearcher<TDocument> {

	/**
	 * Searches for a single term
	 * @param query
	 * @return
	 */
	List<TDocument> search(String query);
	
	/**
	 * Searches for multiple terms
	 * @param query
	 * @return
	 */
	List<TDocument> search(Collection<String> query);
	
	/**
	 * Looks up a specific term (no normalisation is done)
	 * @param query
	 * @return
	 */
	List<TDocument> lookup(String query);
}
