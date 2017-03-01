package de.uni_mannheim.informatik.wdi.datafusion.conflictresolution;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;
import de.uni_mannheim.informatik.wdi.model.DefaultSchemaElement;
import de.uni_mannheim.informatik.wdi.model.FusableValue;
import de.uni_mannheim.informatik.wdi.model.FusedValue;
import de.uni_mannheim.informatik.wdi.usecase.movies.model.Movie;

public class VotingTest extends TestCase {

	public void testResolveConflict() {
		Voting<Double, Movie, DefaultSchemaElement> crf = new Voting<>();
		List<FusableValue<Double, Movie, DefaultSchemaElement>> cluster1 = new ArrayList<>();
		cluster1.add(new FusableValue<Double, Movie, DefaultSchemaElement>(1.0, null, null));
		cluster1.add(new FusableValue<Double, Movie, DefaultSchemaElement>(1.0, null, null));
		cluster1.add(new FusableValue<Double, Movie, DefaultSchemaElement>(3.0, null, null));
		FusedValue<Double, Movie, DefaultSchemaElement> resolvedValue = crf
				.resolveConflict(cluster1);
		assertEquals(1.0, resolvedValue.getValue());
	}

	public void testResolveConflict2() {
		Voting<Double, Movie, DefaultSchemaElement> crf = new Voting<>();
		List<FusableValue<Double, Movie, DefaultSchemaElement>> cluster1 = new ArrayList<>();
		cluster1.add(new FusableValue<Double, Movie, DefaultSchemaElement>(1.0, null, null));
		cluster1.add(new FusableValue<Double, Movie, DefaultSchemaElement>(1.0, null, null));
		cluster1.add(new FusableValue<Double, Movie, DefaultSchemaElement>(3.0, null, null));
		cluster1.add(new FusableValue<Double, Movie, DefaultSchemaElement>(3.0, null, null));
		cluster1.add(new FusableValue<Double, Movie, DefaultSchemaElement>(3.0, null, null));
		FusedValue<Double, Movie, DefaultSchemaElement> resolvedValue = crf
				.resolveConflict(cluster1);
		assertEquals(3.0, resolvedValue.getValue());
	}
	
	public void testResolveConflict3() {
		Voting<Double, Movie, DefaultSchemaElement> crf = new Voting<>();
		List<FusableValue<Double, Movie, DefaultSchemaElement>> cluster1 = new ArrayList<>();
		FusedValue<Double, Movie, DefaultSchemaElement> resolvedValue = crf
				.resolveConflict(cluster1);
		assertEquals(null, resolvedValue.getValue());
	}

}
