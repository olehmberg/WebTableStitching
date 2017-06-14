package de.uni_mannheim.informatik.dws.tnt.match.schemamatching.determinants;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableDeterminant;
import de.uni_mannheim.informatik.dws.winter.matching.rules.FilteringMatchingRule;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;

public class DeterminantMatchingRule extends FilteringMatchingRule<MatchableTableDeterminant, MatchableTableColumn> {

	public DeterminantMatchingRule(double finalThreshold) {
		super(finalThreshold);
	}

	private static final long serialVersionUID = 1L;

	@Override
	public double compare(MatchableTableDeterminant record1, MatchableTableDeterminant record2,
			Correspondence<MatchableTableColumn, Matchable> schemaCorrespondence) {
		return 0;
	}

	@Override
	public Correspondence<MatchableTableDeterminant, MatchableTableColumn> apply(MatchableTableDeterminant record1,
			MatchableTableDeterminant record2, Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCorrespondences) {
		
		
		if(record1.getDataSourceIdentifier()!=record2.getDataSourceIdentifier()) {
			
			// check if both determinants are completely covered by the schema correspondences
			Map<MatchableTableColumn,MatchableTableColumn> leftCovered = new HashMap<>();
			Map<MatchableTableColumn,MatchableTableColumn> rightCovered = new HashMap<>();
			for(Correspondence<MatchableTableColumn, Matchable> cor : schemaCorrespondences.get()) {
				
				if(record1.getColumns().contains(cor.getFirstRecord())) {
					leftCovered.put(cor.getFirstRecord(), cor.getSecondRecord());
				}
				if(record2.getColumns().contains(cor.getSecondRecord())) {
					rightCovered.put(cor.getSecondRecord(), cor.getFirstRecord());
				}
			}
			
			if(rightCovered.values().containsAll(record1.getColumns()) && leftCovered.values().containsAll(record2.getColumns())) {

				MatchableTableDeterminant leftProp = new MatchableTableDeterminant(record1.getTableId(), leftCovered.size()>rightCovered.size() ? leftCovered.keySet() : new HashSet<>(rightCovered.values()));
				MatchableTableDeterminant rightProp = new MatchableTableDeterminant(record2.getTableId(), rightCovered.size()>leftCovered.size() ? rightCovered.keySet() :  new HashSet<>(leftCovered.values()));
				
				return new Correspondence<MatchableTableDeterminant, MatchableTableColumn>(
					leftProp, 
					rightProp, 
					1.0, 
					schemaCorrespondences.filter(
						(c)->
						(leftProp.getColumns().contains(c.getFirstRecord()) || rightProp.getColumns().contains(c.getFirstRecord())) 
						&& (rightProp.getColumns().contains(c.getSecondRecord()) || leftProp.getColumns().contains(c.getSecondRecord()))
						));
			} else {
				return null;
			}
			
		} else {
			return null;
		}
	}

}
