package de.uni_mannheim.informatik.dws.t2k.match.comparators;

import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableColumn;
import de.uni_mannheim.informatik.dws.t2k.match.data.MatchableTableRow;
import de.uni_mannheim.informatik.wdi.matching.Comparator;
import de.uni_mannheim.informatik.wdi.model.Correspondence;
import de.uni_mannheim.informatik.wdi.similarity.SimilarityMeasure;

public class SchemaLabelComparator extends Comparator<MatchableTableColumn, MatchableTableRow>{

	private static final long serialVersionUID = 1L;
	private SimilarityMeasure<String> similarity = null;
	private double sim;
	
	public SchemaLabelComparator(SimilarityMeasure<String> measure) {
		super();
		this.similarity = measure;
	}

/**(non-Javadoc)
 * @see de.uni_mannheim.informatik.wdi.matching.Comparator#compare(Object, Object, Correspondence)
 * 
 * @param schemaCorrespondences here this parameter can be null
 */
	@Override
	public double compare(
			MatchableTableColumn record1,
			MatchableTableColumn record2,
			Correspondence<MatchableTableRow, MatchableTableColumn> schemaCorrespondences) {
		sim = similarity.calculate(record1.getHeader(), record2.getHeader());

		return sim;
	}

}
