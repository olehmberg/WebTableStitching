package de.uni_mannheim.informatik.dws.t2t.tabledance;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

public class Graph<T> {

	public HashMap<String, Node<T>> labels2Nodes; 
	public ArrayList<Edge<T>> edges; 
	public double objective;
	
	
	public Graph() {
		this.labels2Nodes = new HashMap<String, Node<T>>();
		this.edges = new ArrayList<>();
	}
	
	public void addWeightedEdge(String n1Label, String n2Label, T node1, T node2, double d) {
		Node<T> n1 = this.getOrCreateNode(n1Label, node1);
		Node<T> n2 = this.getOrCreateNode(n2Label, node2);
		n1.addWeightedEdge(n2, d);
		n2.addWeightedEdge(n1, d);
		this.edges.add(new Edge<>(n1, n2, d));
	}
	
	public Node<T> getOrCreateNode(String nLabel, T data) {
		Node<T> n;
		if (this.labels2Nodes.containsKey(nLabel)) {
			n = labels2Nodes.get(nLabel);
		}
		else {
			n = new Node<T>(nLabel, data);
			this.labels2Nodes.put(nLabel, n);
		}
		return n;
	}

	public void addNegatedEdge(String n1Label, String n2Label, T data1, T data2) {
		Node<T> n1 = this.getOrCreateNode(n1Label, data1);
		Node<T> n2 = this.getOrCreateNode(n2Label, data2);
		n1.addNegatedEdge(n2);
		n2.addNegatedEdge(n1);
	}

	public HashMap<Node<T>, Node<T>> computeCliques() {
		Collections.sort(edges);
		// maps node ids to their clique
		HashMap<Node<T>, Node<T>> cliques = new HashMap<>();
		objective = 0.0;
		for (Edge<T> e : edges) {
			Node<T> n1 = e.n1;
			Node<T> n2 = e.n2;
			// BOTH ARE ALREADY IN
			if (cliques.containsKey(n1) && cliques.containsKey(n2))  {
				// they are in the same clique
				if (inSameClique(cliques, n1, n2)) {
					objective += e.weight;
				}
				// they are in different cliques (JOIN)
				else {
					Node<T> n1A = this.getAmbassador(cliques, n1);
					Node<T> n2A = this.getAmbassador(cliques, n2);
					// fine
					if (!n1A.negatedEdges.contains(n2) && !n2A.negatedEdges.contains(n1)) {
						cliques.put(n2A, n1A);
						n1A.addNegatedEdges(n2A.negatedEdges);
						objective += e.weight;
					}
					else {
						//  joining these cliques is not allowed
					}
				}
			}
			// FIRST IS OUT SECOND IN (swap them)
			if (!cliques.containsKey(n1) && cliques.containsKey(n2))  {
				Node<T> temp = n1;
				n1 = n2;
				n2 = temp;
			}
			// FIRST IS IN SECOND NOT
			if (cliques.containsKey(n1) && !cliques.containsKey(n2))  {
				Node<T> n1A = this.getAmbassador(cliques, n1);
				if (!n1A.negatedEdges.contains(n2)) {
					cliques.put(n2, n1A);
					n1A.addNegatedEdges(n2.negatedEdges);
					objective += e.weight;
				}
				else {
					//  putting n2 to n1 clique is not allowed
					cliques.put(n2, n2);
				}				
			}
			// NONE OF THEM IS YET IN
			if ((!(cliques.containsKey(n1))) && !(cliques.containsKey(n2)))  {
				// n1.cliqueAmbassador = n1;
				// n2.cliqueAmbassador = n1;
				cliques.put(n1, n1);
				cliques.put(n2, n1);
				n1.addNegatedEdges(n2.negatedEdges);
				objective += e.weight;
			}

		}
		return cliques;
		
	}
	
	public boolean inSameClique(HashMap<Node<T>, Node<T>> cliques, Node<T> n1, Node<T> n2) {
		Node<T> n1A = this.getAmbassador(cliques, n1);
		Node<T> n2A = this.getAmbassador(cliques, n2);
		if (n1A.equals(n2A)) {
			return true;
		}
		else {
			return false;
		}
	}

	public Node<T> getAmbassador(HashMap<Node<T>, Node<T>> cliques, Node<T> n) {
		Node<T> ambassador = n;
		while (!cliques.get(ambassador).equals(ambassador)) {
			ambassador = cliques.get(ambassador);
		}
		return ambassador;
	}
	
}
