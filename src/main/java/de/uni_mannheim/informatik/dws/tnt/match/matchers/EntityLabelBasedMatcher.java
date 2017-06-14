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

public class EntityLabelBasedMatcher extends TableToTableMatcher {

	@Override
	protected void runMatching() {
		// get initial schema correspondences
		Processable<Correspondence<MatchableTableColumn, Matchable>> schemaCors = runValueBased(web);
		
		// match determinants
		printHeadline("Entity-Label Schema Matching");
		Processable<Correspondence<MatchableTableDeterminant, MatchableTableColumn>> detCors = matchEntityLabels(web, schemaCors);
		
		// find duplicates
		printHeadline("Duplicate Detection");
		Processable<Correspondence<MatchableTableRow, MatchableTableDeterminant>> duplicates = findDuplicates(web, detCors);
		
		// run duplicate-based schema matching via determinants
		printHeadline("Entity-Label-based Schema Matching");
		Processable<Correspondence<MatchableTableColumn, MatchableTableRow>> duplicateBasedSchemaCors = runDuplicateBasedSchemaMatching(web, duplicates);
		
		schemaCorrespondences = Correspondence.toMatchable(duplicateBasedSchemaCors);
	}
	
	protected Processable<Correspondence<MatchableTableDeterminant, MatchableTableColumn>> matchEntityLabels(WebTables web, Processable<Correspondence<MatchableTableColumn, Matchable>> cors) {
		DeterminantMatcher matcher = new DeterminantMatcher();
		DataSet<MatchableTableDeterminant, MatchableTableColumn> determinants = matcher.createEntityLabels(web.getTables().values(), web.getSchema());
		
		Processable<Correspondence<MatchableTableDeterminant, MatchableTableColumn>> determinantCors = matcher.propagateDependencies(cors, determinants);
		
		return determinantCors;
	}

}
