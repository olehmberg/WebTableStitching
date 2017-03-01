package de.uni_mannheim.informatik.dws.t2k.match.keygenerator;

import de.uni_mannheim.informatik.wdi.processing.Function;
import de.uni_mannheim.informatik.wdi.processing.Group;

public class T2KMatchGroupJoinKeyGenerator<KeyType, RecordType> implements Function<KeyType, RecordType>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@SuppressWarnings("unchecked")
	@Override
	public KeyType execute(RecordType input) {
		// TODO Auto-generated method stub
		return (KeyType) ((Group<?,?>)input).getKey();
	}

}
