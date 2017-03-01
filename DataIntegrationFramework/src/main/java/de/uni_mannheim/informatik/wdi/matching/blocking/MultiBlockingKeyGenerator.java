package de.uni_mannheim.informatik.wdi.matching.blocking;

import java.io.Serializable;
import java.util.Collection;
import de.uni_mannheim.informatik.wdi.model.Matchable;

public abstract class MultiBlockingKeyGenerator<RecordType extends Matchable> implements Serializable {

	private static final long serialVersionUID = 1L;

	/**
	 * Returns the blocking key for the given record
	 * 
	 * @param entity
	 * @return
	 */
	
	public abstract Collection<String> getMultiBlockingKey(RecordType instance);
}
