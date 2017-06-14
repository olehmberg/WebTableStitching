package de.uni_mannheim.informatik.dws.tnt.match.matchers;

import de.uni_mannheim.informatik.dws.winter.processing.ProcessableCollection;

public class LabelBasedMatcher extends TableToTableMatcher {

	@Override
	protected void runMatching() {
		
		// initialise the schema correspondences with an empty collection (label-based matching is executed during refinement anyways)
		schemaCorrespondences = new ProcessableCollection<>();
		
	}

}
