package de.uni_mannheim.informatik.dws.t2k.utils.io;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import de.uni_mannheim.informatik.dws.t2k.utils.data.Pair;

public class DefaultStringStringStorage extends DataStorageStringString {

	@Override
	public Map<String, Collection<String>> createMapCollection() {
		return new HashMap<String, Collection<String>>();
	}
	
	@Override
	public Collection<String> createCollection2() {
		return new LinkedList<String>();
	}

	@Override
	public Collection<Pair<String, String>> createPairCollection() {
		return new LinkedList<Pair<String,String>>();
	}
	
}
