package de.uni_mannheim.informatik.dws.tnt.match.schemamatching.duplicatebased;

import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.matching.rules.VotingMatchingRule;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;

/**
 * 
 * Determines if the two given columns have the same value in the record combination given by the instance correspondence.
 * Creates a vote for matching values that is grouped by both columns and the determiant correspondence that created the instance correspondence
 * 
 * @author Oliver
 *
 */
public class DuplicateBasedSchemaVotingRule extends VotingMatchingRule<MatchableTableColumn, MatchableTableRow> {

	public DuplicateBasedSchemaVotingRule(double finalThreshold) {
		super(finalThreshold);
	}

	private static final long serialVersionUID = 1L;

	@Override
	public double compare(MatchableTableColumn record1, MatchableTableColumn record2,
			Correspondence<MatchableTableRow, Matchable> correspondence) {
		
		if(record1.getDataSourceIdentifier()!=record2.getDataSourceIdentifier()) {
			MatchableTableRow row1 = null;
			MatchableTableRow row2 = null;
			
			if(record1.getDataSourceIdentifier()==correspondence.getFirstRecord().getDataSourceIdentifier()) {
				row1 = correspondence.getFirstRecord();
				row2 = correspondence.getSecondRecord();
			} else {
				row2 = correspondence.getFirstRecord();
				row1 = correspondence.getSecondRecord();
			}		
			
			Object value1 = row1.get(record1.getColumnIndex());
			Object value2 = row2.get(record2.getColumnIndex());
			
//			if(record1.getTableId()==0 && record2.getTableId()==3
//					&& row1.getRowNumber()==10 && row2.getRowNumber()==12
//					&& record2.getHeader().equals("precio") && record1.getHeader().equals("precio")) {
//			
//			System.out.println(String.format("{%d}%s<->{%d}%s: %s ?? %s", 
//					record1.getTableId(), 
//					record1.getHeader(),
//					record2.getTableId(), 
//					record2.getHeader(),
//					value1,
//					value2));
//			
//			}
				
			return Q.equals(value1, value2, true) ? 1.0 : 0.0;
		} else {
			return 0.0;
		}
	}

	@Override
	protected Pair<MatchableTableColumn, MatchableTableColumn> generateAggregationKey(
			Correspondence<MatchableTableColumn, MatchableTableRow> cor) {
		
		// the correspondence that we get here is created from the result of the compare method.
		// it connects the two schema elements, and has the correspondence that is the duplicate as cause.
		// in turn, this duplicate is created from a determinant, and this is the one that we want to group by
		Correspondence<MatchableTableRow, Matchable> duplicate = Q.firstOrDefault(cor.getCausalCorrespondences().get());
		
		Correspondence<Matchable, Matchable> det = Q.firstOrDefault(duplicate.getCausalCorrespondences().get());
		
//		if(cor.getFirstRecord().getTableId()==0 && cor.getSecondRecord().getTableId()==3
//				&& duplicate.getFirstRecord().getRowNumber()==10 && duplicate.getSecondRecord().getRowNumber()==12
//				&& cor.getFirstRecord().getHeader().equals("precio") && cor.getSecondRecord().getHeader().equals("precio")) {
//		
//		System.out.println(String.format("{%d}%s<->{%d}%s: %s", 
//				cor.getFirstRecord().getTableId(), 
//				cor.getFirstRecord().getHeader(),
//				cor.getSecondRecord().getTableId(), 
//				cor.getSecondRecord().getHeader(),
//				det.getIdentifiers()));
//		
//		}
		
		if(cor.getCausalCorrespondences().size()>1) {
			System.out.println("More than one duplicate for this pair!");
		}
		
		if(duplicate.getCausalCorrespondences().size()>1) {
			System.out.println("More than one determinant for this duplicate!");
		}
		
//		MatchableTableColumn col1 = cor.getFirstRecord();
//		MatchableTableColumn col2 = cor.getSecondRecord();
//		
//		if(col2.getDataSourceIdentifier()<col1.getDataSourceIdentifier()) {
//			MatchableTableColumn c = col1;
//			col1 = col2;
//			col2 = c;
//		}
		
		return new PairWithDeterminant<MatchableTableColumn, MatchableTableColumn, Correspondence<Matchable, Matchable>>(
//				col1,
//				col2,
				cor.getFirstRecord(), 
				cor.getSecondRecord(), 
				det);
	}
}
