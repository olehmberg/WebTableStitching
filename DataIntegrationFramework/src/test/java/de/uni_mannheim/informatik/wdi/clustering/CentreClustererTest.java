package de.uni_mannheim.informatik.wdi.clustering;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;

import junit.framework.TestCase;
import de.uni_mannheim.informatik.wdi.clustering.CentreClusterer;
import de.uni_mannheim.informatik.wdi.model.Triple;

public class CentreClustererTest extends TestCase {

	public void testCluster() {

		ArrayList<Triple<String, String, Double>> similarityGraph = new ArrayList<Triple<String, String, Double>>();
		similarityGraph.add(new Triple<String, String, Double>("hello",
				"hello1", 0.9));
		similarityGraph.add(new Triple<String, String, Double>("hello",
				"hello2", 0.9));
		similarityGraph.add(new Triple<String, String, Double>("hello", "hi",
				0.1));
		similarityGraph.add(new Triple<String, String, Double>("hello", "hi1",
				0.1));
		similarityGraph.add(new Triple<String, String, Double>("hello", "hi2",
				0.1));
		similarityGraph.add(new Triple<String, String, Double>("hello1",
				"hello2", 0.9));
		similarityGraph.add(new Triple<String, String, Double>("hello1", "hi",
				0.1));
		similarityGraph.add(new Triple<String, String, Double>("hello1", "hi1",
				0.1));
		similarityGraph.add(new Triple<String, String, Double>("hello1", "hi2",
				0.1));
		similarityGraph.add(new Triple<String, String, Double>("hello2", "hi",
				0.1));
		similarityGraph.add(new Triple<String, String, Double>("hello2", "hi1",
				0.1));
		similarityGraph.add(new Triple<String, String, Double>("hello2", "hi2",
				0.1));
		similarityGraph
				.add(new Triple<String, String, Double>("hi", "hi1", 0.9));
		similarityGraph
				.add(new Triple<String, String, Double>("hi", "hi2", 0.9));
		similarityGraph.add(new Triple<String, String, Double>("hi1", "hi2",
				0.9));

		CentreClusterer<String> cc = new CentreClusterer<String>();
		Map<String, Collection<String>> cluster = cc.cluster(similarityGraph);
		assertEquals(2, cluster.size());
	}
	
	public void testCluster2() {

		ArrayList<Triple<String, String, Double>> similarityGraph = new ArrayList<Triple<String, String, Double>>();
		similarityGraph.add(new Triple<String, String, Double>("hello",
				"hello1", 0.9));
		similarityGraph.add(new Triple<String, String, Double>("hello",
				"hello2", 0.9));
		similarityGraph.add(new Triple<String, String, Double>("hello", "hi",
				0.1));
		similarityGraph.add(new Triple<String, String, Double>("hello", "hi1",
				0.1));
		similarityGraph.add(new Triple<String, String, Double>("hello", "hi2",
				0.1));
		similarityGraph.add(new Triple<String, String, Double>("hello1",
				"hello2", 0.9));
		similarityGraph.add(new Triple<String, String, Double>("hello1", "hi",
				0.1));
		similarityGraph.add(new Triple<String, String, Double>("hello1", "hi1",
				0.1));
		similarityGraph.add(new Triple<String, String, Double>("hello1", "hi2",
				0.1));
		similarityGraph.add(new Triple<String, String, Double>("hello2", "hi",
				0.1));
		similarityGraph.add(new Triple<String, String, Double>("hello2", "hi1",
				0.1));
		similarityGraph.add(new Triple<String, String, Double>("hello2", "hi2",
				0.1));
		similarityGraph
				.add(new Triple<String, String, Double>("hi", "hi1", 0.9));
		similarityGraph
				.add(new Triple<String, String, Double>("hi", "hi2", 0.9));
		similarityGraph.add(new Triple<String, String, Double>("hi1", "hi2",
				0.9));
		
		similarityGraph.add(new Triple<String, String, Double>("hello1",
				"hello", 0.9));
		similarityGraph.add(new Triple<String, String, Double>("hello2",
				"hello", 0.9));
		similarityGraph.add(new Triple<String, String, Double>("hi", "hello",
				0.1));
		similarityGraph.add(new Triple<String, String, Double>("hi1", "hello",
				0.1));
		similarityGraph.add(new Triple<String, String, Double>("hi2", "hello",
				0.1));
		similarityGraph.add(new Triple<String, String, Double>("hello2",
				"hello1", 0.9));
		similarityGraph.add(new Triple<String, String, Double>("hi", "hello1",
				0.1));
		similarityGraph.add(new Triple<String, String, Double>("hi1", "hello1",
				0.1));
		similarityGraph.add(new Triple<String, String, Double>("hi2", "hello1",
				0.1));
		similarityGraph.add(new Triple<String, String, Double>("hi", "hello2",
				0.1));
		similarityGraph.add(new Triple<String, String, Double>("hi1", "hello2",
				0.1));
		similarityGraph.add(new Triple<String, String, Double>("hi2", "hello2",
				0.1));
		similarityGraph
				.add(new Triple<String, String, Double>("hi1", "hi", 0.9));
		similarityGraph
				.add(new Triple<String, String, Double>("hi2", "hi", 0.9));
		similarityGraph.add(new Triple<String, String, Double>("hi2", "hi1",
				0.9));

		CentreClusterer<String> cc = new CentreClusterer<String>();
		Map<String, Collection<String>> cluster = cc.cluster(similarityGraph);
		assertEquals(2, cluster.size());
	}

}
