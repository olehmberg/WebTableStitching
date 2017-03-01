package de.uni_mannheim.informatik.wdi.datafusion.conflictresolution.numeric;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;
import de.uni_mannheim.informatik.wdi.model.DefaultSchemaElement;
import de.uni_mannheim.informatik.wdi.model.FusableValue;
import de.uni_mannheim.informatik.wdi.model.FusedValue;
import de.uni_mannheim.informatik.wdi.usecase.movies.model.Movie;

public class AverageTest extends TestCase {

	public void testResolveConflictCollectionOfFusableValueOfDoubleRecordType() {

		Average<Movie, DefaultSchemaElement> crf = new Average<>();
		List<FusableValue<Double, Movie, DefaultSchemaElement>> cluster1 = new ArrayList<>();
		cluster1.add(new FusableValue<Double, Movie, DefaultSchemaElement>(1.0, null, null));
		cluster1.add(new FusableValue<Double, Movie, DefaultSchemaElement>(2.0, null, null));
		cluster1.add(new FusableValue<Double, Movie, DefaultSchemaElement>(3.0, null, null));
		FusedValue<Double, Movie, DefaultSchemaElement> resolvedValue = crf
				.resolveConflict(cluster1);
		assertEquals(2.0, resolvedValue.getValue());
	}

	public void testResolveConflictCollectionOfFusableValueOfDoubleRecordType2() {

		Average<Movie, DefaultSchemaElement> crf = new Average<>();
		List<FusableValue<Double, Movie, DefaultSchemaElement>> cluster2 = new ArrayList<>();
		FusedValue<Double, Movie, DefaultSchemaElement> resolvedValue = crf
				.resolveConflict(cluster2);
		assertEquals(null, resolvedValue.getValue());
	}

}
