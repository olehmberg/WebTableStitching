package de.uni_mannheim.informatik.dws.t2t.tabledance;

import java.util.HashMap;
import java.util.HashSet;

public class Node<T> {
	
	public String label;
	public long id;
	public T data;

	public HashMap<Node<T>, Double> weightedEdges;
	public HashSet<Node<T>> negatedEdges;
	
	// this points to the guy who knows it all
	public Node<T> cliqueAmbassador = null;
	
	public static long instanceCounter = 0;
	
	public Node(String label, T data) {
		this.data = data;
		this.label = label;
		this.id = instanceCounter;
		instanceCounter++;
		this.weightedEdges = new HashMap<Node<T>, Double>();
		this.negatedEdges = new HashSet<Node<T>>();
	}
	
	public int hashcode() {
		return (int)this.id;
	}
	
	public boolean equals(Node<T> that) {
		if (!(that instanceof Node)) {
			return false;
		}
		else {
			Node<T> thatNode = (Node<T>)that;
			if (thatNode.id == this.id) {
				return true;
			}
			else {
				return false;
			}
		}	
	}

	public void addWeightedEdge(Node<T> n, double d) {
		this.weightedEdges.put(n, d);
	}
	
	
	public void addNegatedEdge(Node<T> n) {
		this.negatedEdges.add(n);
	}
	
	public void addNegatedEdges(HashSet<Node<T>> negatedEdges) {
		this.negatedEdges.addAll(negatedEdges);
		
	}
	
	public String toString() {
		return this.label;
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return label.hashCode();
	}

	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public boolean equals(Object obj) {
		return label.equals(((Node<T>)obj).label);
	}

}
