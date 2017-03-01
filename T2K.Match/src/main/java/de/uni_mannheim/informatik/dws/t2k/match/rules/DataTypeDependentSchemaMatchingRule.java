package de.uni_mannheim.informatik.dws.t2k.match.rules;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import de.uni_mannheim.informatik.dws.t2k.datatypes.DataType;
import de.uni_mannheim.informatik.dws.t2k.match.comparators.MatchableTableRowComparator;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Q;
import de.uni_mannheim.informatik.wdi.matching.SchemaMatchingRuleWithVoting;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.DataProcessingEngine;
import de.uni_mannheim.informatik.wdi.processing.DatasetIterator;
import de.uni_mannheim.informatik.wdi.processing.Group;
import de.uni_mannheim.informatik.wdi.processing.RecordKeyValueMapper;
import de.uni_mannheim.informatik.wdi.processing.RecordMapper;

public class DataTypeDependentSchemaMatchingRule extends SchemaMatchingRuleWithVoting<MatchableTableRow, MatchableTableColumn, MatchableTableColumn> {

	private static final long serialVersionUID = -2211353700799708359L;
	private HashMap<DataType, MatchableTableRowComparator<?>> comparators;
//	private de.uni_mannheim.informatik.wdi.matching.Comparator<MatchableTableRow, MatchableTableColumn> keyComparator;
	private int numVotesPerValue = 1;
	private int numCorrespondences = 1;
	private int rdfsLabelId = -1;
	
	/**
	 * @param rdfsLabelId the rdfsLabelId to set
	 */
	public void setRdfsLabelId(int rdfsLabelId) {
		this.rdfsLabelId = rdfsLabelId;
	}
	
//	private Map<Integer, Map<Integer, Integer>> propertyIndices;
	
	public HashMap<DataType, MatchableTableRowComparator<?>> getComparators() {
		return comparators;
	}

	public int getRdfsLabelId() {
		return rdfsLabelId;
	}

	public void setComparatorForType(DataType type, MatchableTableRowComparator<?> comparator) {
		comparators.put(type, comparator);
	}

	public DataTypeDependentSchemaMatchingRule(double finalThreshold) {
		//, Map<Integer, Map<Integer, Integer>> propertyIndices
		super(finalThreshold);
		comparators = new HashMap<>();
//		this.propertyIndices = propertyIndices;
	}

	/**
	 * @param numCorrespondences the numCorrespondences to set
	 */
	public void setNumCorrespondences(int numCorrespondences) {
		this.numCorrespondences = numCorrespondences;
	}
	/**
	 * @return the numCorrespondences
	 */
	public int getNumCorrespondences() {
		return numCorrespondences;
	}
	
	/**
	 * @param numVotesPerValue the numVotesPerValue to set
	 */
	public void setNumVotesPerValue(int numVotesPerValue) {
		this.numVotesPerValue = numVotesPerValue;
	}
	/**
	 * @return the numVotesPerValue
	 */
	public int getNumVotesPerValue() {
		return numVotesPerValue;
	}
	
	protected ResultSet<Pair<Correspondence<MatchableTableRow, MatchableTableColumn>, Correspondence<MatchableTableColumn, MatchableTableRow>>> filterVotes(ResultSet<Pair<Correspondence<MatchableTableRow, MatchableTableColumn>,Correspondence<MatchableTableColumn, MatchableTableRow>>> results, DataProcessingEngine processingEngine) {
		RecordKeyValueMapper<List<String>, Pair<Correspondence<MatchableTableRow, MatchableTableColumn>, Correspondence<MatchableTableColumn, MatchableTableRow>>, Pair<Correspondence<MatchableTableRow, MatchableTableColumn>, Correspondence<MatchableTableColumn, MatchableTableRow>>> groupByVotingValue = new RecordKeyValueMapper<List<String>, Pair<Correspondence<MatchableTableRow,MatchableTableColumn>,Correspondence<MatchableTableColumn,MatchableTableRow>>, Pair<Correspondence<MatchableTableRow,MatchableTableColumn>,Correspondence<MatchableTableColumn,MatchableTableRow>>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(
					Pair<Correspondence<MatchableTableRow, MatchableTableColumn>, Correspondence<MatchableTableColumn, MatchableTableRow>> record,
					DatasetIterator<Pair<List<String>, Pair<Correspondence<MatchableTableRow, MatchableTableColumn>, Correspondence<MatchableTableColumn, MatchableTableRow>>>> resultCollector) {

				// use the row and column identifiers from the web table as grouping key
				resultCollector.next(new Pair<List<String>, Pair<Correspondence<MatchableTableRow,MatchableTableColumn>,Correspondence<MatchableTableColumn,MatchableTableRow>>>(
						Q.toList(record.getFirst().getFirstRecord().getIdentifier(), record.getSecond().getFirstRecord().getIdentifier()), 
						record));
				
			}
		};
		ResultSet<Group<List<String>, Pair<Correspondence<MatchableTableRow, MatchableTableColumn>, Correspondence<MatchableTableColumn, MatchableTableRow>>>> groupedByValue = processingEngine.groupRecords(results, groupByVotingValue);
		
		RecordMapper<Group<List<String>, Pair<Correspondence<MatchableTableRow, MatchableTableColumn>, Correspondence<MatchableTableColumn, MatchableTableRow>>>, Pair<Correspondence<MatchableTableRow, MatchableTableColumn>, Correspondence<MatchableTableColumn, MatchableTableRow>>> maxNumberOfVotes = new RecordMapper<Group<List<String>,Pair<Correspondence<MatchableTableRow,MatchableTableColumn>,Correspondence<MatchableTableColumn,MatchableTableRow>>>, Pair<Correspondence<MatchableTableRow,MatchableTableColumn>,Correspondence<MatchableTableColumn,MatchableTableRow>>>() {
			
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public void mapRecord(
					Group<List<String>, Pair<Correspondence<MatchableTableRow, MatchableTableColumn>, Correspondence<MatchableTableColumn, MatchableTableRow>>> record,
					DatasetIterator<Pair<Correspondence<MatchableTableRow, MatchableTableColumn>, Correspondence<MatchableTableColumn, MatchableTableRow>>> resultCollector) {
				
				// record.getRecords() contains all votes of a specific value
				
				// sort them by similarity value
				List<Pair<Correspondence<MatchableTableRow, MatchableTableColumn>, Correspondence<MatchableTableColumn, MatchableTableRow>>> sorted = Q.sort(record.getRecords().get(), new Comparator<Pair<Correspondence<MatchableTableRow, MatchableTableColumn>, Correspondence<MatchableTableColumn, MatchableTableRow>>>() {

					@Override
					public int compare(
							Pair<Correspondence<MatchableTableRow, MatchableTableColumn>, Correspondence<MatchableTableColumn, MatchableTableRow>> o1,
							Pair<Correspondence<MatchableTableRow, MatchableTableColumn>, Correspondence<MatchableTableColumn, MatchableTableRow>> o2) {
						return -Double.compare(o1.getSecond().getSimilarityScore(), o2.getSecond().getSimilarityScore());
					}
				});
				
				// use the top K votes
				int voted = 0;
				
				while(voted < Math.min(numVotesPerValue, sorted.size())) {
					resultCollector.next(sorted.get(voted++));
				}
				
			}
		};
		return processingEngine.transform(groupedByValue, maxNumberOfVotes);
	}
	
	@Override
	public ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> aggregate(
			ResultSet<Pair<Correspondence<MatchableTableRow, MatchableTableColumn>,Correspondence<MatchableTableColumn, MatchableTableRow>>> results, int numVotes, DataProcessingEngine processingEngine) {
		
		ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> result = new ResultSet<>();
		
		// the voting step: every correspondence in results is a vote
		
		// filtering step: each value has a limited number of votes
		ResultSet<Pair<Correspondence<MatchableTableRow, MatchableTableColumn>, Correspondence<MatchableTableColumn, MatchableTableRow>>> filteredVotes = null;
		if(getNumVotesPerValue()!=0) {
			filteredVotes = filterVotes(results, processingEngine);
		} else {
			filteredVotes = results;
		}

		// maps each column to all candidate properties and their summed vote weight
		HashMap<MatchableTableColumn, HashMap<MatchableTableColumn, Double>> sums = new HashMap<>();
		// maps each column to all candidate properties and their vote count
		HashMap<MatchableTableColumn, HashMap<MatchableTableColumn, Integer>> counts = new HashMap<>();
		
		// count correspondences for each combination and sum up the scores (weighted by instance scores)
		for(Pair<Correspondence<MatchableTableRow, MatchableTableColumn>,Correspondence<MatchableTableColumn, MatchableTableRow>> pair : filteredVotes.get()) {

			// pair.getFirst() is the vote on record level (the rows that voted)
			// pair.getSecond() is the vote on schema level (the columns that were voted for)
			
			Correspondence<MatchableTableColumn, MatchableTableRow> c = pair.getSecond();
			
			
			HashMap<MatchableTableColumn, Double> sumMap = sums.get(c.getFirstRecord());
			if(sumMap==null) {
				sumMap = new HashMap<>();
				sums.put(c.getFirstRecord(), sumMap);
			}
			
			Double sum = sumMap.get(c.getSecondRecord());
			if(sum==null) {
				sum = 0.0;
			}
			
			// sum up the (weighted by instance correspondence score) scores for this combination of columns
			sum += c.getSimilarityScore() * pair.getFirst().getSimilarityScore();
			sumMap.put(c.getSecondRecord(), sum);
			
			HashMap<MatchableTableColumn, Integer> countMap = counts.get(c.getFirstRecord());
			if(countMap==null) {
				countMap = new HashMap<>();
				counts.put(c.getFirstRecord(), countMap);
			}
			
			Integer count = countMap.get(c.getSecondRecord());
			if(count==null) {
				count = 0;
			}
			
			// count the number of correspondences for this combination of columns
			count++;
			countMap.put(c.getSecondRecord(), count);
		}
		
		
		// calculate the summed score and normalise by correspondence count
		for(MatchableTableColumn c1 : sums.keySet()) {

			// c1 is a column from a web table
			
			List<Correspondence<MatchableTableColumn, MatchableTableRow>> correspondences = new ArrayList<>();
			
			for(MatchableTableColumn c2 : sums.get(c1).keySet()) {
				
				// c2 is a candidate property
				
				//TODO this normalisation does not take the size of the table into account
				// a) use number of correspondences as approximation for table size
				// b) use actual table size (additional parameter?)
				
				Double value = sums.get(c1).get(c2) / (double)counts.get(c1).get(c2);
				
				// generate a correspondences if the score exceeds the similarity threshold
				if(value>=getFinalThreshold()) {
					Correspondence<MatchableTableColumn, MatchableTableRow> cor = new Correspondence<>(c1, c2, value, null);
					correspondences.add(cor);
					
//					System.out.println(String.format("[ACCEPT] [%d]%s<->%s (%.6f)", c1.getColumnIndex(), c1.getHeader(), c2.getHeader(), value));
				} else {
//					System.out.println(String.format("[DECLINE] [%d]%s<->%s (%.6f)", c1.getColumnIndex(), c1.getHeader(), c2.getHeader(), value));
				}
			}
			
			// sort all correspondences that were created for the current left-hand-side column c1
			Collections.sort(correspondences, new Comparator<Correspondence<MatchableTableColumn, MatchableTableRow>>() {

				@Override
				public int compare(Correspondence<MatchableTableColumn, MatchableTableRow> o1, Correspondence<MatchableTableColumn, MatchableTableRow> o2) {
					return -Double.compare(o1.getSimilarityScore(), o2.getSimilarityScore());
				}
			});
			
			// choose the top n (=those that received the highest numbers of votes) as final correspondences
			for(int i = 0; i < getNumCorrespondences() && i < correspondences.size(); i++) {
				Correspondence<MatchableTableColumn, MatchableTableRow> cor = correspondences.get(i);
				
				result.add(cor);
				
//				System.out.println(String.format("[CHOOSE] [%d]%s<->%s", cor.getFirstRecord().getColumnIndex(), cor.getFirstRecord().getHeader(), cor.getSecondRecord().getHeader()));
			}
		}
		
		return result;
	}

	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.matching.SchemaMatchingRuleWithVoting#apply(de.uni_mannheim.informatik.wdi.model.Matchable, de.uni_mannheim.informatik.wdi.model.Matchable, de.uni_mannheim.informatik.wdi.model.Correspondence)
	 */
	@Override
	public Correspondence<MatchableTableColumn, MatchableTableRow> apply(MatchableTableColumn schemaElement1,
			MatchableTableColumn schemaElement2,
			Correspondence<MatchableTableRow, MatchableTableColumn> correspondence) {

		// get the comparator for this data type
		MatchableTableRowComparator<?> cmp = comparators.get(schemaElement1.getType());
		
		int idx1 = schemaElement1.getColumnIndex();
		int idx2 = schemaElement2.getColumnIndex();
		
		if(rdfsLabelId==-1 || schemaElement2.getColumnIndex()!=rdfsLabelId) { // do not match the keys here
			if(cmp!=null) {
				// make sure the property exists in the second record and both columns have the same data type
//				if(cmp.canCompareRecords(correspondence.getFirstRecord(), correspondence.getSecondRecord(), idx1, idx2)) {
				if(cmp.canCompareRecords(correspondence.getFirstRecord(), correspondence.getSecondRecord(), schemaElement1, schemaElement2)) {
					
					// calculate the similarity value
//					double score = cmp.compare(correspondence.getFirstRecord(), correspondence.getSecondRecord(), idx1, idx2);
					double score = cmp.compare(correspondence.getFirstRecord(), correspondence.getSecondRecord(), schemaElement1, schemaElement2);
					
					return new Correspondence<MatchableTableColumn, MatchableTableRow>(schemaElement1, schemaElement2, score, null);
				} else {
					// also occurs if one of the values is null
	//				System.out.println(String.format("[MISMATCH] cannot compare [%d]%s (%s) <-> %s (%s)", schemaElement1.getColumnIndex(), schemaElement1.getHeader(), schemaElement1.getType(), schemaElement2.getHeader(), schemaElement2.getType()));
				}
			} else {
				System.out.println(String.format("[MISSING] no comparator for [%d]%s (%s)", schemaElement1.getColumnIndex(), schemaElement1.getHeader(), schemaElement1.getType(), schemaElement2.getHeader(), schemaElement2.getType()));
			}
		} else {
//			keyComparator.compare(correspondence.getFirstRecord(), correspondence.getSecondRecord(), new Correspondence<MatchableTableColumn, MatchableTableRow>(schemaElement1, schemaElement2, 1.0, null));
		}
		
		return null;
	}

}
