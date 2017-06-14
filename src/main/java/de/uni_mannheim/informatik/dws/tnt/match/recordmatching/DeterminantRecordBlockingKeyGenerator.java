package de.uni_mannheim.informatik.dws.tnt.match.recordmatching;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableDeterminant;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.winter.matching.blockers.generators.BlockingKeyGenerator;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.model.Pair;
import de.uni_mannheim.informatik.dws.winter.processing.DataIterator;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;
import de.uni_mannheim.informatik.dws.winter.utils.StringUtils;
import de.uni_mannheim.informatik.dws.winter.utils.query.Q;

/**
 * 
 * Creates blocking keys from the values of matched determinants
 * 
 * @author Oliver
 *
 */
public class DeterminantRecordBlockingKeyGenerator extends BlockingKeyGenerator<MatchableTableRow, MatchableTableDeterminant, MatchableTableRow> {

	private static final long serialVersionUID = 1L;

	@Override
	public void generateBlockingKeys(MatchableTableRow record,
			Processable<Correspondence<MatchableTableDeterminant, Matchable>> correspondences,
			DataIterator<Pair<String, MatchableTableRow>> resultCollector) {

		if(correspondences!=null) {

			// get a set of all determinants that have matches (and will hence be used to generate a blocking key)
			// the same determinant can be matched to many tables, which will lead to replication of the blocking key values if we use all correspondences
			Set<MatchableTableDeterminant> determinants = new HashSet<>();
			for(Correspondence<MatchableTableDeterminant, Matchable> cor : correspondences.get()) {
				
				MatchableTableDeterminant det = null;
				
				if(cor.getFirstRecord().getTableId()==record.getTableId()) {
					det = cor.getFirstRecord();
				} else {
					det = cor.getSecondRecord();
				}
				
				determinants.add(det);
			}
			
			// generate a blocking key for every matched determinant
			for(MatchableTableDeterminant det : determinants) {
				
//				// blocking key from original code:
//				List<Integer> indices = Q.sort(Q.project(det.getColumns(), new MatchableTableColumn.ColumnIndexProjection()));
//				List<Object> values = Arrays.asList(record.get(Q.toPrimitiveIntArray(indices)));
//				
//				String blockingKey = StringUtils.join(Q.toString(values), "/");

				// get the record's values for the determinant's attributes
				Object[] values = record.get(Q.toPrimitiveIntArray(Q.project(det.getColumns(), new MatchableTableColumn.ColumnIndexProjection())));
				
				// sort the values to make sure that they match, even if the attributes in the different records have a different order
				List<String> sorted = Q.sort(Q.toString(Arrays.asList(values)));
				
				// create the blocking key
				String blockingKey = StringUtils.join(sorted, "/");
				
//				System.out.println(String.format("{%d} %s\t%s", record.getDataSourceIdentifier(), record.getIdentifier(), blockingKey));
				
				// create the result
				resultCollector.next(new Pair<String, MatchableTableRow>(blockingKey, record));
				
			}
			
//			
//			for(Correspondence<MatchableTableDeterminant, Matchable> cor : correspondences.get()) {
//			
//				MatchableTableDeterminant det = null;
//				
//				if(cor.getFirstRecord().getTableId()==record.getTableId()) {
//					det = cor.getFirstRecord();
//				} else {
//					det = cor.getSecondRecord();
//				}
//							
//				// get a map from the table with the lower id to the table with the higher id
//				Map<Matchable, Matchable> leftToRightIds = new HashMap<>();
//				int leftId=-1;
//				for(Correspondence<Matchable, Matchable> c : cor.getCausalCorrespondences().get()) {
//					if(c.getFirstRecord().getDataSourceIdentifier()<c.getSecondRecord().getDataSourceIdentifier()) {
//						leftToRightIds.put(c.getFirstRecord(), c.getSecondRecord());
//						leftId = c.getFirstRecord().getDataSourceIdentifier();
//					} else {
//						leftToRightIds.put(c.getSecondRecord(), c.getFirstRecord());
//						leftId = c.getSecondRecord().getDataSourceIdentifier();
//					}
//				}
//				
//				// sort the values by the column ids of the table with the lower id (so we have the same order of columns for both tables)
//				List<Matchable> leftSorted = Q.sort(leftToRightIds.keySet(), (m1, m2) -> Integer.compare(m1.getDataSourceIdentifier(), m2.getDataSourceIdentifier()));
//				List<Matchable> rightSorted = new ArrayList<>(leftSorted.size());
//				for(Matchable m : leftSorted) {
//					rightSorted.add(leftToRightIds.get(m));
//				}
//				
//				// get a map of the current determinant's columns by column id
//				Map<String, MatchableTableColumn> colsById = new HashMap<>();
//				for(MatchableTableColumn c : det.getColumns()) {
//					colsById.put(c.getIdentifier(), c);
//				}
//				
//				List<Matchable> myColumns = leftId == record.getDataSourceIdentifier() ? leftSorted : rightSorted;
//				List<MatchableTableColumn> detCols = new ArrayList<>(myColumns.size());
//				for(Matchable m : myColumns) {
//					detCols.add(colsById.get(m.getIdentifier()));
//				}
//				
//				// use the values of the determinant's attributes as blocking key
//				int[] indices = Q.toPrimitiveIntArray(Q.project(detCols, new MatchableTableColumn.ColumnIndexProjection()));
//				String value = StringUtils.join(Q.toString(Q.toList(record.get(indices))), "/");
//			
//				resultCollector.next(new Pair<String, MatchableTableRow>(value, record));
//				
////				System.out.println(String.format("{%d} %s\t%s", record.getDataSourceIdentifier(), record.getIdentifier(), value));
//			}
		}
	}

}
