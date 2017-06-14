/** 
 *
 * Copyright (C) 2015 Data and Web Science Group, University of Mannheim, Germany (code@dwslab.de)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 		http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package de.uni_mannheim.informatik.dws.tnt.match.evaluation;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;

import de.uni_mannheim.informatik.dws.winter.model.Performance;

/**
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class ClusteringPerformance {

	private Map<String, Double> clusterPrecision;
	private Map<String, Double> classRecall;
	private Performance modelTheoreticPerformance;
	private Performance correspondencePerformance;
	
	public int getNumIncorrectCorrespondences() {
		if(correspondencePerformance==null) {
			return 0;
		} else {
			return correspondencePerformance.getNumberOfPredicted()-correspondencePerformance.getNumberOfCorrectlyPredicted();
		}
	}
	
	protected void setClusterPrecision(Map<String, Double> clusterPrecision) {
		this.clusterPrecision = clusterPrecision;
	}

	protected void setClassRecall(Map<String, Double> classRecall) {
		this.classRecall = classRecall;
	}

	protected void setModelTheoreticPerformance(Performance modelTheoreticPerformance) {
		this.modelTheoreticPerformance = modelTheoreticPerformance;
	}

	public Map<String, Double> getClusterPrecision() {
		return clusterPrecision;
	}

	public Map<String, Double> getClassRecall() {
		return classRecall;
	}

	public Performance getModelTheoreticPerformance() {
		return modelTheoreticPerformance;
	}
	
	/**
	 * @return the correspondencePerformance
	 */
	public Performance getCorrespondencePerformance() {
		return correspondencePerformance;
	}
	/**
	 * @param correspondencePerformance the correspondencePerformance to set
	 */
	public void setCorrespondencePerformance(Performance correspondencePerformance) {
		this.correspondencePerformance = correspondencePerformance;
	}
	
	private Map<String, Performance> performanceByCluster;
	private Performance weightedAverage;
	private Performance microAverage;
	private Performance macroAverage;
	private double clusteringPrecision;
	private double penalisedClusteringPrecision;

	public Map<String, Performance> getPerformanceByCluster() {
		return performanceByCluster;
	}

	public Performance getWeightedAverage() {
		return weightedAverage;
	}

	public Performance getMicroAverage() {
		return microAverage;
	}

	public Performance getMacroAverage() {
		return macroAverage;
	}
	
	public double getClusteringPrecision() {
		return clusteringPrecision;
	}

	public double getPenalisedClusteringPrecision() {
		return penalisedClusteringPrecision;
	}

	public ClusteringPerformance(Map<String, Performance> performanceByCluster) {
		this.performanceByCluster = performanceByCluster;
		calculatePerformance();
	}
	
	protected void calculatePerformance() {
		int correctSum = 0;
    	int predictedSum = 0;
    	int totalSum = 0;
    	
    	double pr = 0.0;
    	double rec = 0.0;

    	double clusteringPrecisionSum = 0.0;
    	int clusteringPrecisionCount = 0;
    	int gsClusters = 0;
    	
    	int totalCors = 0;
    	for(String key : performanceByCluster.keySet()) {
    		Performance p = performanceByCluster.get(key);
    		totalCors += p.getNumberOfCorrectTotal();
    	}
    	
    	double precisionSum = 0.0;
    	int precisionCount = 0;
    	double recallSum = 0.0;
    	for(String key : performanceByCluster.keySet()) {
    		Performance p = performanceByCluster.get(key);
    		
			correctSum += p.getNumberOfCorrectlyPredicted();
			predictedSum += p.getNumberOfPredicted();
    		totalSum += p.getNumberOfCorrectTotal(); // == totalCors
    		    		
    		// calculate weighted average
    		double weight = (p.getNumberOfCorrectTotal() / (double)totalCors);
    		rec += p.getRecall() * weight; 
    		pr += p.getPrecision() * weight;
    		
    		// calculate clustering precision (take into account the number of clusters)
    		if(p.getNumberOfPredicted()>1 && p.getNumberOfCorrectTotal()>0) {
    			BigDecimal numPairs = new BigDecimal((p.getNumberOfCorrectlyPredicted())).multiply(new BigDecimal(p.getNumberOfCorrectlyPredicted()-1.0)).divide(new BigDecimal(2.0));
    			BigDecimal ttlPairs = new BigDecimal(p.getNumberOfPredicted()).multiply(new BigDecimal(p.getNumberOfPredicted()-1.0)).divide(new BigDecimal(2.0));
    			numPairs = numPairs.setScale(10);
	    		clusteringPrecisionSum += numPairs.divide(ttlPairs, RoundingMode.HALF_UP).doubleValue();
	    		clusteringPrecisionCount++;
    		}
    		
    		if(p.getNumberOfCorrectTotal()>0) {
    			gsClusters++;
    		}
    		
    		if(p.getNumberOfPredicted()>1) {	
    			// don't consider clusters of size 1 as predictions
    			// there is no correspondence, they will be considered in recall
    		
    			precisionSum += p.getPrecision();
    			precisionCount++;
    		}
    		recallSum += p.getRecall();
    	}
    	
    	microAverage = new Performance(correctSum, predictedSum, totalSum);
    	macroAverage = new IRPerformance(precisionSum/(double)precisionCount, recallSum/(double)performanceByCluster.size());
    	
    	// this way of evaluation does not take incorrect clusters (which were not matched to the gold standard) into account
    	// these clusters will not influence precision, but only recall
    	weightedAverage = new IRPerformance(pr, rec);
    	
    	
    	clusteringPrecision = clusteringPrecisionSum / (double)clusteringPrecisionCount;
    	double penaltyFactor = 0.0;
    	if(performanceByCluster.size()<gsClusters) {
    		penaltyFactor = performanceByCluster.size()/(double)gsClusters;
    	} else {
    		penaltyFactor = gsClusters/(double)performanceByCluster.size();
    	}
    	penalisedClusteringPrecision = penaltyFactor * clusteringPrecision;
	}
	
	public String format(boolean summaryOnly) {
		StringBuilder sb = new StringBuilder();
    	
		sb.append("*** Cluster-based Evaluation ***\n");
		
    	Performance p = microAverage;
    	sb.append("Micro Average Performance\n");
    	sb.append(String.format("\tcorrect: %d\n\tcreated: %d\n\ttotal: %d\n", p.getNumberOfCorrectlyPredicted(), p.getNumberOfPredicted(), p.getNumberOfCorrectTotal()));
    	sb.append(String.format("\t%s %.4f\n", org.apache.commons.lang3.StringUtils.rightPad("Precision:",10), p.getPrecision()));
    	sb.append(String.format("\t%s %.4f\n", org.apache.commons.lang3.StringUtils.rightPad("Recall:",10), p.getRecall()));
    	sb.append(String.format("\t%s %.4f\n", org.apache.commons.lang3.StringUtils.rightPad("F1:",10), p.getF1()));
    	
    	p = macroAverage;
    	sb.append("Macro Average Performance\n");
    	sb.append(String.format("\t%s %.4f\n", org.apache.commons.lang3.StringUtils.rightPad("Precision:",10), p.getPrecision()));
    	sb.append(String.format("\t%s %.4f\n", org.apache.commons.lang3.StringUtils.rightPad("Recall:",10), p.getRecall()));
    	sb.append(String.format("\t%s %.4f\n", org.apache.commons.lang3.StringUtils.rightPad("F1:",10), p.getF1()));
    	
    	sb.append("Weighted Average Performance over all ground truth clusters\n");
    	p = weightedAverage;
    	sb.append(String.format("\t%s %.4f\n", org.apache.commons.lang3.StringUtils.rightPad("Precision:",10), p.getPrecision()));
    	sb.append(String.format("\t%s %.4f\n", org.apache.commons.lang3.StringUtils.rightPad("Recall:",10), p.getRecall()));
    	sb.append(String.format("\t%s %.4f\n", org.apache.commons.lang3.StringUtils.rightPad("F1:",10), p.getF1()));
    	
    	sb.append(String.format("Clustering Precision: %.4f\n", clusteringPrecision));
    	sb.append(String.format("Penalised Clustering Precision: %.4f\n", penalisedClusteringPrecision));
    	
    	sb.append("Model Theoretic Performance\n");
    	p = modelTheoreticPerformance;
    	if(p!=null) {
	    	sb.append(String.format("\t%s %.10f\n", org.apache.commons.lang3.StringUtils.rightPad("Precision:",10), p.getPrecision()));
	    	sb.append(String.format("\t%s %.10f\n", org.apache.commons.lang3.StringUtils.rightPad("Recall:",10), p.getRecall()));
	    	sb.append(String.format("\t%s %.10f\n", org.apache.commons.lang3.StringUtils.rightPad("F1:",10), p.getF1()));
    	} else {
    		sb.append("n/a\n");
    	}
    	
    	sb.append("*** Correspondence-based Evaluation ***\n");
    	
    	sb.append("Pair-wise Correspondences\n");
    	if(correspondencePerformance!=null) {
	    	p = correspondencePerformance;
	    	sb.append(String.format("\t%d incorrect correspondence(s)\n", p.getNumberOfPredicted()-p.getNumberOfCorrectlyPredicted()));
	    	sb.append(String.format("\t%s %.10f\n", org.apache.commons.lang3.StringUtils.rightPad("Precision:",10), p.getPrecision()));
	    	sb.append(String.format("\t%s %.10f\n", org.apache.commons.lang3.StringUtils.rightPad("Recall:",10), p.getRecall()));
	    	sb.append(String.format("\t%s %.10f\n", org.apache.commons.lang3.StringUtils.rightPad("F1:",10), p.getF1()));
    	} else {
    		sb.append("n/a\n");
    	}
    	
    	return sb.toString();
	}
}
