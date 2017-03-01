package de.uni_mannheim.informatik.dws.t2k.match;

import java.io.Serializable;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import de.uni_mannheim.informatik.dws.t2k.match.data.KnowledgeBase;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.t2k.match.recordmapper.ClassDecisionCorrespondenceToCorrespondenceRecordMapper;
import de.uni_mannheim.informatik.dws.t2k.match.recordmapper.ClassDistributionDataAggregator;
import de.uni_mannheim.informatik.dws.t2k.match.recordmapper.ClassDistributionRecordKeyMapper;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;

public class ClassDecision implements Serializable{

	
	/**
	 * @author Sanikumar
	 * @author oliver
	 */
	private static final long serialVersionUID = 1L;


	public ClassDecision() {

	}


	private Map<Integer, Set<String>> classDist = new HashMap<Integer, Set<String>>();
	private HashMap<Integer, Double> classWeight = new HashMap<Integer, Double>();
	
	
	public Map<Integer, Set<String>> runClassDecision(KnowledgeBase kb, ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> instanceCorrespondences, DataProcessingEngine proc){
		
//    	calculate class weight for each class from Knowledge-Base
		classWeight = kb.getClassWeight();
		
		ClassDecisionCorrespondenceToCorrespondenceRecordMapper classDecisionCorrespondenceToCorrespondenceRecordMapper = new ClassDecisionCorrespondenceToCorrespondenceRecordMapper(classWeight);
		
		instanceCorrespondences = proc.transform(instanceCorrespondences, classDecisionCorrespondenceToCorrespondenceRecordMapper);
		
//        choose top candidate for each instance by taking one to one mapping
        ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> oneToOneInstanceCorrespondences = TopKMatch.getTopKMatch(instanceCorrespondences, proc, 1, 0.0);
        
//        count class distribution per table
         classDist = getClassDistribution(oneToOneInstanceCorrespondences, kb, proc);
        
		return classDist;
		
	}
	
	public Map<Integer, Set<String>> getClassDistribution(ResultSet<Correspondence<MatchableTableRow, MatchableTableColumn>> correspondences, final KnowledgeBase kb, DataProcessingEngine proc) {
		
		Map<Integer, Set<String>> classesPerTable = new HashMap<Integer, Set<String>>();
		
		final Map<Integer, String> classIndices = kb.getClassIndices();
		
		ClassDistributionRecordKeyMapper classDistributionRecordKeyMapper = new ClassDistributionRecordKeyMapper();
		
		ClassDistributionDataAggregator classDistributionDataAggregator = new ClassDistributionDataAggregator(classIndices);
		
		// counts the number of candidates per class?
		// <table id, class name -> number of candidates> ?
		ResultSet<Pair<Integer, HashMap<String, Double>>> candidates = proc.aggregateRecords(correspondences, classDistributionRecordKeyMapper, classDistributionDataAggregator);
		
		
		for(Pair<Integer, HashMap<String, Double>> pair : candidates.get()) {
			HashSet<String> classes = new HashSet<String>();
			final HashMap<String, Double> nclassCounts = normalize(pair.getSecond());
			
//			prune all classes below similarity 0.5
			for(Map.Entry<String, Double> entry : nclassCounts.entrySet())
			{
				if(entry.getValue() >= 0.5){
					classes.add(entry.getKey());
				}
			}
			
			if(classesPerTable.isEmpty()){

				// if no class meets the similarity threshold, choose the top 5
				
				List<Map.Entry<String, Double>> classesSortedByFrequency = Q.sort(nclassCounts.entrySet(), new Comparator<Map.Entry<String, Double>>() {

					@Override
					public int compare(Entry<String, Double> o1, Entry<String, Double> o2) {
						return -Double.compare(o1.getValue(), o2.getValue());
					}});
				
				for(Map.Entry<String, Double> entry : classesSortedByFrequency) {
					
					classes.add(entry.getKey());
					if(classes.size() > 4)
						break;
				}
			}
			
//			for(String cls : classes) {
//				System.out.println(String.format("Selected class '%s' with weight %.6f", cls, nclassCounts.get(cls)));
//			}
			
//			//TODO this is the final class decision and should be done later, but only here we have the weights ...
//			String max = Q.max(nclassCounts.keySet(), new Func<Double, String>() {
//
//				@Override
//				public Double invoke(String in) {
//					return nclassCounts.get(in);
//				}
//			});
//			
//			classes.clear();
//			classes.add(max);
//			System.out.println(String.format("Final class is '%s' with weight %.6f", max, nclassCounts.get(max)));
			
			classesPerTable.put(pair.getFirst(), classes);
		}
		
		return classesPerTable;
	
	}
	
//	gives the normalized class counts
	public  HashMap<String, Double> normalize(HashMap<String, Double> classCounts){
		
		Double max = 0.0;

		for (Map.Entry<String, Double> entry : classCounts.entrySet())
		{
		    if (entry.getValue() > max)
		    {
		        max = entry.getValue();
		    }
		    else
		    	continue;
		}
		
		for (Map.Entry<String, Double> entry : classCounts.entrySet())
		{
			classCounts.put(entry.getKey(), entry.getValue()/max);
		}
		
		return classCounts;
		
	}
}
