package de.uni_mannheim.informatik.dws.tnt.match.matchers;

import de.uni_mannheim.informatik.dws.winter.model.Correspondence;

public class ValueBasedMatcher extends TableToTableMatcher {

	@Override
	protected void runMatching() {
		
		schemaCorrespondences = Correspondence.toMatchable(runValueBased(web));
		
	}

}
