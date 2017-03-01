package de.uni_mannheim.informatik.dws.t2k.match.rules;

import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.wdi.matching.Comparator;
import de.uni_mannheim.informatik.wdi.matching.MatchingRule;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.model.DefaultRecord;
import de.uni_mannheim.informatik.wdi.model.FeatureVectorDataSet;
import de.uni_mannheim.informatik.wdi.model.ResultSet;

public class CandidateSelectionRule extends MatchingRule<MatchableTableRow, MatchableTableColumn> {

	private static final long serialVersionUID = 1L;
	private Comparator<MatchableTableRow, MatchableTableColumn> comparator = null;

	private int rdfsLabelId;
    
	public Comparator<MatchableTableRow, MatchableTableColumn> getComparator() {
		return comparator;
	}

	public int getRdfsLabelId() {
		return rdfsLabelId;
	}

	/**
	 * @param comparator the comparator to set
	 */
	public void setComparator(Comparator<MatchableTableRow, MatchableTableColumn> comparator) {
		this.comparator = comparator;
	}
	
	public CandidateSelectionRule(double finalThreshold, int rdfsLabelId) {
		super(finalThreshold);
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
		
//		create schema correspondences between the key columns and rdfs:Label
		Correspondence<MatchableTableColumn, MatchableTableRow> keyCorrespondence = null;
		for(Correspondence<MatchableTableColumn, MatchableTableRow> cor :schemaCorrespondences.get()) {
			if(cor.getSecondRecord().getColumnIndex()==rdfsLabelId) {
				keyCorrespondence = cor;
				break;
			}
		}
		
//		calculate similarity
		double sim = comparator.compare(record1, record2, keyCorrespondence);
		
//		if the similarity satisfies threshold then return the correspondence between two records, 'null' otherwise
		if(sim >= getFinalThreshold()) {
			return createCorrespondence(record1, record2, schemaCorrespondences, sim);
		} else {
			return null;
		}
	}

}
