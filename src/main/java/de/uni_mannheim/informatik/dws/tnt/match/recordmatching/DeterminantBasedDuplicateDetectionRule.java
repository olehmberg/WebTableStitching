package de.uni_mannheim.informatik.dws.tnt.match.recordmatching;

import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableDeterminant;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.matching.rules.FilteringMatchingRule;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.HashedDataSet;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;

public class DeterminantBasedDuplicateDetectionRule extends FilteringMatchingRule<MatchableTableRow, MatchableTableDeterminant> {

	public DeterminantBasedDuplicateDetectionRule(double finalThreshold) {
		super(finalThreshold);
	}

	private static final long serialVersionUID = 1L;

	@Override
	public double compare(MatchableTableRow record1, MatchableTableRow record2,
			Correspondence<MatchableTableDeterminant, Matchable> schemaCorrespondence) {
		return 0;
	}

	@Override
	public Correspondence<MatchableTableRow, MatchableTableDeterminant> apply(MatchableTableRow record1,
			MatchableTableRow record2,
			Processable<Correspondence<MatchableTableDeterminant, Matchable>> schemaCorrespondences) {
		
		if(record1.getDataSourceIdentifier()!=record2.getDataSourceIdentifier()) {
			// the blocker produces a pair if any of the determinant correspondences produces a match
			// now we have to find out which ones matched and create correspondences
			
			Processable<Correspondence<MatchableTableDeterminant, Matchable>> matches = new ProcessableCollection<>();
			
			for(Correspondence<MatchableTableDeterminant, Matchable> cor : schemaCorrespondences.get()) {
				
				boolean match = true;
				
				// get the relevant parts of the records' schemas from the determinants
				DataSet<MatchableTableColumn, Matchable> schema1 = null;
				DataSet<MatchableTableColumn, Matchable> schema2 = null;
				if(cor.getFirstRecord().getDataSourceIdentifier()==record1.getDataSourceIdentifier()) {
					schema1 = new HashedDataSet<>(cor.getFirstRecord().getColumns());
					schema2 = new HashedDataSet<>(cor.getSecondRecord().getColumns());
				} else {
					schema2 = new HashedDataSet<>(cor.getFirstRecord().getColumns());
					schema1 = new HashedDataSet<>(cor.getSecondRecord().getColumns());
				}
				
				// compare the values of all columns in the matched determinants
				for(Correspondence<Matchable, Matchable> colCor : cor.getCausalCorrespondences().get()) {
					
					Object value1, value2;
					MatchableTableColumn col1=null, col2=null;
					
					if(colCor.getFirstRecord().getDataSourceIdentifier()==record1.getDataSourceIdentifier()) {
						col1 = schema1.getRecord(colCor.getFirstRecord().getIdentifier());
						col2 = schema2.getRecord(colCor.getSecondRecord().getIdentifier());
					} else {
						col2 = schema1.getRecord(colCor.getFirstRecord().getIdentifier());
						col1 = schema2.getRecord(colCor.getSecondRecord().getIdentifier());
					}
					
					value1 = record1.get(col1.getColumnIndex());
					value2 = record2.get(col2.getColumnIndex());
					
//					if(!(value1==null && value2==null) 
//							&& !(value1==null && value2!=null || value1!=null && value2==null)
//							&& !(value1.equals(value2))) {
					if(!Q.equals(value1, value2, true)) {
						match = false;
						break;
					}
				}
				
				if(match) {
					matches.add(cor);
				}
				
			}
			
			if(matches.size()>0) {
				return new Correspondence<MatchableTableRow, MatchableTableDeterminant>(
					record1, 
					record2, 
					matches.size()/(double)schemaCorrespondences.size(), 
					matches);
			} 
		} 
			
		return null;
	}

}
