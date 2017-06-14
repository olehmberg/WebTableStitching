package de.uni_mannheim.informatik.dws.tnt.match.matchers;

import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableDeterminant;
import de.uni_mannheim.informatik.dws.tnt.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.dws.tnt.match.data.WebTables;
import de.uni_mannheim.informatik.dws.tnt.match.schemamatching.determinants.DeterminantMatcher;
import de.uni_mannheim.informatik.dws.winter.model.Correspondence;
import de.uni_mannheim.informatik.dws.winter.model.DataSet;
import de.uni_mannheim.informatik.dws.winter.model.Matchable;
import de.uni_mannheim.informatik.dws.winter.processing.Processable;

public class DeterminantBasedMatcher extends TableToTableMatcher {

	@Override
	protected void runMatching() {
		// get initial schema correspondences
		Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCors = runValueBased(web);
		
		// match determinants
		printHeadline("Determinant Schema Matching");
		Processable<Correspondence<MatchableTableDeterminant, MatchableTableColumn>> detCors = matchDeterminants(web, schemaCors);
		
		// find duplicates
		printHeadline("Duplicate Detection");
		Processable<Correspondence<MatchableTableRow, MatchableTableDeterminant>> duplicates = findDuplicates(web, detCors);
		
		//TODO figure out which correspondences could be checked with duplicates and which couldn't
		// if we don't find duplicates, the duplicate-based matcher cannot create a correspondence
		// but that does not necessarily mean that the correspondence is wrong, but it won't be in the result
		
		// run duplicate-based schema matching via determinants
		printHeadline("Determinant-based Schema Matching");
		Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> duplicateBasedSchemaCors = runDuplicateBasedSchemaMatching(web, duplicates);
		
		schemaCorrespondences = Correspondence.toMatchable(duplicateBasedSchemaCors);
		
		Correspondence.setDirectionByDataSourceIdentifier(schemaCorrespondences);
	}

	protected Processable<Correspondence<MatchableTableDeterminant, MatchableTableColumn>> matchDeterminants(WebTables web, Processable<Correspondence<MatchableTableColumn, Matchable>> cors) {
		DeterminantMatcher matcher = new DeterminantMatcher();
		DataSet<MatchableTableDeterminant, MatchableTableColumn> determinants = matcher.createFDs(0, web.getTables().values(), web.getSchema());
		
//		logFDsWithCors(determinants, cors);
		
		Processable<Correspondence<MatchableTableDeterminant, MatchableTableColumn>> determinantCors = matcher.propagateDependencies(cors, determinants);
		
//		List<Integer> debugTables = new LinkedList<>();
//		debugTables.add(web.getTableIndices().get("12.json"));
//		debugTables.add(web.getTableIndices().get("18.json"));
//		debugTables.add(web.getTableIndices().get("28.json"));
//		
//		for(Correspondence<MatchableTableDeterminant, MatchableTableColumn> cor : determinantCors
//				.filter( (c1) -> debugTables.contains(c1.getFirstRecord().getTableId()) || debugTables.contains(c1.getSecondRecord().getTableId()) )
//				.sort((c)->c.getFirstRecord().getTableId()).get()) {
//			
//			System.out.println(String.format("[%d]{%s}<->[%d]{%s}",
//					cor.getFirstRecord().getTableId(),
//					StringUtils.join(Q.project(cor.getFirstRecord().getColumns(), new MatchableTableColumn.ColumnHeaderProjection()), ','),
//					cor.getSecondRecord().getTableId(),
//					StringUtils.join(Q.project(cor.getSecondRecord().getColumns(), new MatchableTableColumn.ColumnHeaderProjection()), ',')
//					));
//			for(Correspondence<MatchableTableColumn, Matchable> cause : cor.getCausalCorrespondences().get()) {
//				System.out.println(String.format("\t{%d}%s<->{%d}%s", 
//						cause.getFirstRecord().getTableId(),
//						cause.getFirstRecord().getHeader(),
//						cause.getSecondRecord().getTableId(),
//						cause.getSecondRecord().getHeader()));
//			}
//		}
		
		return determinantCors;
	}
}
