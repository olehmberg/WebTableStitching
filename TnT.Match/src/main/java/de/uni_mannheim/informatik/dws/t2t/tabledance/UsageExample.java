package de.uni_mannheim.informatik.dws.t2t.tabledance;

import java.util.HashMap;

public class UsageExample {

	public static void main(String[] args) {
		
		Graph<String> g = new Graph<String>();

		g.addWeightedEdge("A", "B", null, null, 0.5);
		g.addWeightedEdge("B", "C", null, null, 0.44);
		g.addWeightedEdge("A", "C", null, null, 0.33);
		
		g.addWeightedEdge("A", "D", null, null, 0.5);
		
		g.addWeightedEdge("D", "E", null, null, 0.9);
		g.addWeightedEdge("D", "F", null, null, 0.1);
		g.addWeightedEdge("E", "F", null, null, 0.25);
		g.addWeightedEdge("G", "F", null, null, 0.98);
		
		g.addNegatedEdge("F", "C", null, null);
		g.addNegatedEdge("D", "C", null, null);
		g.addNegatedEdge("D", "B", null, null);
		
		
		
		HashMap<Node<String>, Node<String>> cliques = g.computeCliques();
		
		
		
		for (Node<String> n : cliques.keySet()) {
			System.out.println(n + " -> " + g.getAmbassador(cliques, cliques.get(n)));
		}
		
		System.out.println(g.objective);
	
	}
	
	
	

}
