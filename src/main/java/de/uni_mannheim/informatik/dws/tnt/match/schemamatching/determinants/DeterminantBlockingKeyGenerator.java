package de.uni_mannheim.informatik.dws.tnt.match.schemamatching.determinants;

import java.util.HashMap;
import java.util.Map;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableDeterminant;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.generators.BlockingKeyGenerator;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Group;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;

/**
 * 
 * Blocking key generator for {@link MatchableTableDeterminant}s. Concatenates all column ids (sorted) as blocking key.
 * If all columns have correspondences to another table, the column ids of the other table are used to generate an additional blocking key. 
 * 
 * example:
 * 	record is a determinant with columns {a1,a2}
 * correspondences are {a1->b1, a2-b2, a1->c1}
 * 
 * result will be the following blocking keys for record:
 * 	a1/a2
 *  b1/b2
 * 
 * @author Oliver
 *
 */
public class DeterminantBlockingKeyGenerator extends BlockingKeyGenerator<MatchableTableDeterminant, MatchableTableColumn, MatchableTableDeterminant> {

	private static final long serialVersionUID = 1L;

	@Override
	public void generateBlockingKeys(MatchableTableDeterminant record,
			Processable<Correspondence<MatchableTableColumn, Matchable>> correspondences,
			DataIterator<Pair<String, MatchableTableDeterminant>> resultCollector) {
		// correspondences are joined via dataset id by standard blocker, so we get only correspondences for the same table as 'record' in 'correspondences'
		
		// group the correspondences by table combination, so we can check if the determinant is completely mapped for each table individually
		Processable<Group<String, Correspondence<MatchableTableColumn, Matchable>>> groups = correspondences.groupRecords((Correspondence<MatchableTableColumn, Matchable> cor,
				DataIterator<Pair<String, Correspondence<MatchableTableColumn, Matchable>>> col) -> {
					if(cor.getFirstRecord().getTableId()!=cor.getSecondRecord().getTableId()) {
						
						if(cor.getFirstRecord().getTableId()>cor.getSecondRecord().getTableId()) {
							System.err.println("Wrong order!");
						}
						
						col.next(
							new Pair<String, Correspondence<MatchableTableColumn, Matchable>>(
									String.format("%d/%d", cor.getFirstRecord().getTableId(), cor.getSecondRecord().getTableId())
									, cor));
					}
		});
		
		// now iterate over the groups
		groups.iterateDataset(new DataIterator<Group<String,Correspondence<MatchableTableColumn, Matchable>>>() {
			
			private static final long serialVersionUID = 1L;

			@Override
			public void next(Group<String, Correspondence<MatchableTableColumn, Matchable>> group) {
				Map<MatchableTableColumn,MatchableTableColumn> matchedColumnsLeft = new HashMap<>();
				Map<MatchableTableColumn,MatchableTableColumn> matchedColumnsRight = new HashMap<>();
				
				// figure out which columns of the key have a correspondence
				for(Correspondence<MatchableTableColumn, Matchable> cor : group.getRecords().get()) {
					if(record.getColumns().contains(cor.getFirstRecord())) {
						matchedColumnsLeft.put(cor.getFirstRecord(), cor.getSecondRecord());
					} else if(record.getColumns().contains(cor.getSecondRecord())) {
						matchedColumnsRight.put(cor.getSecondRecord(), cor.getFirstRecord());
					}
				}
				
				// check if the key is completely mapped
				Map<MatchableTableColumn,MatchableTableColumn> keyMapping = null;
				if(record.getColumns().equals(matchedColumnsLeft.keySet())) {
					keyMapping = matchedColumnsLeft;
				} else if (record.getColumns().equals(matchedColumnsRight.keySet())) {
					keyMapping = matchedColumnsRight;
				}
				
				if(keyMapping!=null) {
					
					// we have a complete mapping for the key and can generate the blocking keys
					// note: here we can only check if one of the keys is completely covered, so the matching rule has to check the other one
					
					// create one blocking key value for each attribute in the determinant
					for(Map.Entry<MatchableTableColumn, MatchableTableColumn> e : keyMapping.entrySet()) {
						
						resultCollector.next(new Pair<String, MatchableTableDeterminant>(e.getKey().getIdentifier(), record));
						resultCollector.next(new Pair<String, MatchableTableDeterminant>(e.getValue().getIdentifier(), record));
					}
				}
			}
			
			@Override
			public void initialise() {	
			}
			
			@Override
			public void finalise() {
			}
		});
		
	}

}
