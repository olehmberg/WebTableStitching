package de.uni_mannheim.informatik.dws.t2k.match.rules;

import java.util.HashMap;

import de.uni_mannheim.informatik.dws.t2k.datatypes.DataType;
import de.uni_mannheim.informatik.dws.t2k.match.comparators.MatchableTableRowComparator;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.wdi.matching.Comparator;
import de.uni_mannheim.informatik.wdi.matching.MatchingRule;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.DefaultRecord;
import de.uni_mannheim.informatik.wdi.model.FeatureVectorDataSet;
import de.uni_mannheim.informatik.wdi.model.ResultSet;

/**
 * 
 * A record matching rule that uses different comparators for each data type. The key value also has its own comparator and a weight that can be specified.
 * The similarity value is the sum of the similarities of the values weighted by the similaritiy of the schema correspondence between the values.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 *
 */
public class DataTypeDependentRecordMatchingRule extends MatchingRule<MatchableTableRow, MatchableTableColumn> {

	private static final long serialVersionUID = -2211353700799708359L;
	private HashMap<DataType, MatchableTableRowComparator<?>> comparators;
	private Comparator<MatchableTableRow, MatchableTableColumn> keyValueComparator;
	private double keyValueWeight = 1.0/3.0;
	private int rdfsLabelId;
	
	public void setComparatorForType(DataType type, MatchableTableRowComparator<?> comparator) {
		comparators.put(type, comparator);
	}
	
	public HashMap<DataType, MatchableTableRowComparator<?>> getComparators() {
		return comparators;
	}


	public Comparator<MatchableTableRow, MatchableTableColumn> getKeyValueComparator() {
		return keyValueComparator;
	}


	public double getKeyValueWeight() {
		return keyValueWeight;
	}


	public int getRdfsLabelId() {
		return rdfsLabelId;
	}


	/**
	 * @param keyValueComparator the keyValueComparator to set
	 */
	public void setKeyValueComparator(
			Comparator<MatchableTableRow, MatchableTableColumn> keyValueComparator) {
		this.keyValueComparator = keyValueComparator;
	}
	
	/**
	 * @param keyValueWeight the keyValueWeight to set
	 */
	public void setKeyValueWeight(double keyValueWeight) {
		this.keyValueWeight = keyValueWeight;
	}
	
	public DataTypeDependentRecordMatchingRule(double finalThreshold, int rdfsLabelId) {
		super(finalThreshold);
		comparators = new HashMap<>();
		this.rdfsLabelId = rdfsLabelId;
	}

	/**
	 * (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.matching.MatchingRule#generateFeatures(java.lang.Object, java.lang.Object, de.uni_mannheim.informatik.wdi.model.ResultSet, de.uni_mannheim.informatik.wdi.model.FeatureVectorDataSet)
	 */
	@Override
	public DefaultRecord generateFeatures(MatchableTableRow record1,
			MatchableTableRow record2, ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences, FeatureVectorDataSet features) {
		return null;
	}

	/** (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.matching.MatchingRule#apply(java.lang.Object, java.lang.Object, de.uni_mannheim.informatik.wdi.model.ResultSet)
	 */
	@Override
	public Correspondence<MatchableTableRow, MatchableTableColumn> apply(
			MatchableTableRow record1,
			MatchableTableRow record2,
			ResultSet<Correspondence<MatchableTableColumn, MatchableTableRow>> schemaCorrespondences) {
		double sum = 0.0;
		double sumOfWeights = 0.0;

		// sum up the similarity scores for all properties
		for(Correspondence<MatchableTableColumn, MatchableTableRow> sc : schemaCorrespondences.get()) {
			
			if(sc.getSecondRecord().getColumnIndex()!=rdfsLabelId) {
				MatchableTableRowComparator<?> cmp = comparators.get(record1.getType(sc.getFirstRecord().getColumnIndex()));
				if(cmp!=null) {
					int idx1 = sc.getFirstRecord().getColumnIndex();
					int idx2 = sc.getSecondRecord().getColumnIndex();
					
					// check if sc.getSecondRecord() exists as a property in record2
//					if(cmp.canCompareRecords(record1, record2, idx1, idx2)) {
					if(cmp.canCompareRecords(record1, record2, sc.getFirstRecord(), sc.getSecondRecord())) {
//						Double sim = cmp.compare(record1, record2, idx1, idx2);
						Double sim = cmp.compare(record1, record2, sc.getFirstRecord(), sc.getSecondRecord());
						
						double weight = sc.getSimilarityScore();
						
						sum += sim * weight;
						sumOfWeights += sc.getSimilarityScore();
					}
				}
			} else {
				// the key value has a special weight and comparator
				double keyValueSim = keyValueComparator.compare(record1, record2, sc);
				
//				double otherValueSim = comparators.get(DataType.string).compare(record1, record2, sc.getFirstRecord().getColumnIndex(), sc.getSecondRecord().getColumnIndex());
				double otherValueSim = comparators.get(DataType.string).compare(record1, record2, sc.getFirstRecord(), sc.getSecondRecord());
				
				//TODO in the original T2K, the similarity was 1/3 the special comparator and 2/3 the comparator for string values
				keyValueSim = (1.0/3.0) * keyValueSim + (2.0/3.0) * otherValueSim;
				
				sum += keyValueSim * keyValueWeight;
				sumOfWeights += keyValueWeight;
			}
		}
		
//		calculate final similarity. 
//		if sum of the similarity for all properties is '0' then set the similarity to '0', otherwise set the similarity to (sum/sumOfWeights)
		double sim = sum==0.0 ? 0.0 : (sum/sumOfWeights);
		
//		if the similarity satisfies threshold then return the correspondence between two records, 'null' otherwise
		if(sim>=getFinalThreshold()) {
			return createCorrespondence(record1, record2, schemaCorrespondences, sim);
		} else {
			return null;
		}
	}


}
