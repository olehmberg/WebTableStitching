package de.uni_mannheim.informatik.wdi.datafusion.conflictresolution;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;
import de.uni_mannheim.informatik.wdi.model.DefaultSchemaElement;
import de.uni_mannheim.informatik.wdi.model.FusableValue;
import de.uni_mannheim.informatik.wdi.model.FusedValue;
import de.uni_mannheim.informatik.wdi.similarity.string.LevenshteinSimilarity;
import de.uni_mannheim.informatik.wdi.usecase.movies.model.Movie;

public class ClusteredVoteTest extends TestCase {

	public void testResolveConflict() {
		ClusteredVote<String, Movie, DefaultSchemaElement> crf = new ClusteredVote<>(
				new LevenshteinSimilarity(), 0.0);

		List<FusableValue<String, Movie, DefaultSchemaElement>> cluster1 = new ArrayList<>();
		cluster1.add(new FusableValue<String, Movie, DefaultSchemaElement>("hi", null, null));
		cluster1.add(new FusableValue<String, Movie, DefaultSchemaElement>("hi1", null, null));
		cluster1.add(new FusableValue<String, Movie, DefaultSchemaElement>("hello1", null,
				null));
		cluster1.add(new FusableValue<String, Movie, DefaultSchemaElement>("hello", null, null));
		cluster1.add(new FusableValue<String, Movie, DefaultSchemaElement>("hello2", null,
				null));

		FusedValue<String, Movie, DefaultSchemaElement> resolvedValue = crf
				.resolveConflict(cluster1);
		assertEquals("hello1", resolvedValue.getValue());
	}

	public void testResolveConflict1() {
		ClusteredVote<String, Movie, DefaultSchemaElement> crf = new ClusteredVote<>(
				new LevenshteinSimilarity(), 0.0);

		List<FusableValue<String, Movie, DefaultSchemaElement>> cluster1 = new ArrayList<>();

		FusedValue<String, Movie, DefaultSchemaElement> resolvedValue = crf
				.resolveConflict(cluster1);
		assertEquals(null, resolvedValue.getValue());
	}
}
