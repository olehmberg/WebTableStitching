package de.uni_mannheim.informatik.wdi.model;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;

/**
 * Wrapper class for any kind of collection. Can be used to change the way collections are handled internally without modifying other code
 * @author Oliver
 *
 */
public class ResultSet<ElementType> implements BasicCollection<ElementType> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	protected Collection<ElementType> elements;
	
	public ResultSet() {
		elements = new LinkedList<>();
	}
	
	public ResultSet(Collection<ElementType> elements) {
		this.elements = elements;
	}

	public void add(ElementType element) {
		elements.add(element);
	}
	
	public Collection<ElementType> get() {
		return elements;
	}
	
	public int size() {
		return elements.size();
	}
	
	public void merge(ResultSet<ElementType> other) {
		for(ElementType elem : other.get()) {
			add(elem);
		}
	}
	
	public void remove(ElementType element) {
		elements.remove(element);
	}
	
	public void remove(Collection<ElementType> element) {
		elements.removeAll(element);
	}
	
	public void deduplicate() {
		elements = new ArrayList<>(new HashSet<>(elements));
	}
}
