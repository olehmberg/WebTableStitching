package de.uni_mannheim.informatik.wdi.parallel;

import java.util.concurrent.ConcurrentLinkedQueue;

import de.uni_mannheim.informatik.wdi.model.ResultSet;

public class ThreadSafeResultSet<ElementType> extends ResultSet<ElementType> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public ThreadSafeResultSet() {
		elements = new ConcurrentLinkedQueue<ElementType>();
	}

}
