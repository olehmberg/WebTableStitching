package de.uni_mannheim.informatik.dws.t2t.tabledance;

/**
 * Just a helper class, not really strictly related to the graph and its nodes!
 *
 */
public class Edge<T> implements Comparable<Edge<T>>{
	
	public Node<T> n1;
	public Node<T> n2;
	public double weight;
	
	public Edge(Node<T> n1, Node<T> n2, double w) {
		this.n1 = n1;
		this.n2 = n2;
		this.weight = w;
	}

	@Override
	public int compareTo(Edge<T> e) {
		if (e.weight > this.weight) {
			return 1;
		}
		else if (e.weight == this.weight) {
			return 0;
		}
		else {
			return -11;
		}
		
	}
	
	public String toString() {
		return ""  + n1 + ", " + n2 + ": " + weight;
	}


}
