package de.uni_mannheim.informatik.dws.t2t.tabledance;

import java.util.HashSet;

public class Clique<T> {

	public HashSet<Long> nodeIds;
	
	public void addNode(Node<T> n) {
		this.nodeIds.add(n.id);
		
	}
	
	public boolean isCotained(Node<T> n) {
		return nodeIds.contains(n.id);
	}
	
}
