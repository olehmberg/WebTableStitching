package de.uni_mannheim.informatik.wdi.datafusion.conflictresolution.list;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;
import de.uni_mannheim.informatik.wdi.model.DefaultSchemaElement;
import de.uni_mannheim.informatik.wdi.model.FusableValue;
import de.uni_mannheim.informatik.wdi.model.FusedValue;
import de.uni_mannheim.informatik.wdi.usecase.movies.model.Movie;

public class IntersectionTest extends TestCase {

	public void testResolveConflictCollectionOfFusableValueOfListOfValueTypeRecordType() {
		Intersection<String, Movie, DefaultSchemaElement> crf = new Intersection<>();
		List<FusableValue<List<String>, Movie, DefaultSchemaElement>> cluster1 = new ArrayList<>();
		cluster1.add(new FusableValue<List<String>, Movie, DefaultSchemaElement>(
				new ArrayList<String>(), null, null));
		cluster1.add(new FusableValue<List<String>, Movie, DefaultSchemaElement>(
				new ArrayList<String>(), null, null));
		cluster1.add(new FusableValue<List<String>, Movie, DefaultSchemaElement>(
				new ArrayList<String>(), null, null));
		FusedValue<List<String>, Movie, DefaultSchemaElement> resolvedValue = crf
				.resolveConflict(cluster1);
		assertEquals(0, resolvedValue.getValue().size());
	}

	public void testResolveConflictCollectionOfFusableValueOfListOfValueTypeRecordType2() {
		Intersection<String, Movie, DefaultSchemaElement> crf = new Intersection<>();
		List<FusableValue<List<String>, Movie, DefaultSchemaElement>> cluster1 = new ArrayList<>();
		cluster1.add(new FusableValue<List<String>, Movie, DefaultSchemaElement>(Arrays
				.asList("h0", "h1"), null, null));
		cluster1.add(new FusableValue<List<String>, Movie, DefaultSchemaElement>(Arrays
				.asList("h1", "h2"), null, null));
		cluster1.add(new FusableValue<List<String>, Movie, DefaultSchemaElement>(Arrays
				.asList("h2", "h3"), null, null));
		FusedValue<List<String>, Movie, DefaultSchemaElement> resolvedValue = crf
				.resolveConflict(cluster1);
		assertEquals(0, resolvedValue.getValue().size());

	}

	public void testResolveConflictCollectionOfFusableValueOfListOfValueTypeRecordType3() {
		Intersection<String, Movie, DefaultSchemaElement> crf = new Intersection<>();
		List<FusableValue<List<String>, Movie, DefaultSchemaElement>> cluster1 = new ArrayList<>();
		cluster1.add(new FusableValue<List<String>, Movie, DefaultSchemaElement>(Arrays
				.asList("h0", "h1"), null, null));
		cluster1.add(new FusableValue<List<String>, Movie, DefaultSchemaElement>(Arrays
				.asList("h0", "h1"), null, null));
		cluster1.add(new FusableValue<List<String>, Movie, DefaultSchemaElement>(Arrays
				.asList("h0", "h2"), null, null));
		FusedValue<List<String>, Movie, DefaultSchemaElement> resolvedValue = crf
				.resolveConflict(cluster1);
		assertEquals(1, resolvedValue.getValue().size());

	}

	public void testResolveConflictCollectionOfFusableValueOfListOfValueTypeRecordType4() {
		Intersection<String, Movie, DefaultSchemaElement> crf = new Intersection<>();
		List<FusableValue<List<String>, Movie, DefaultSchemaElement>> cluster1 = new ArrayList<>();

		ArrayList<String> list = new ArrayList<String>();
		list.add("h1");
		list.add("h2");
		cluster1.add(new FusableValue<List<String>, Movie, DefaultSchemaElement>(
				new ArrayList<String>(list), null, null));
		cluster1.add(new FusableValue<List<String>, Movie, DefaultSchemaElement>(
				new ArrayList<String>(list), null, null));
		cluster1.add(new FusableValue<List<String>, Movie, DefaultSchemaElement>(
				new ArrayList<String>(list), null, null));
		FusedValue<List<String>, Movie, DefaultSchemaElement> resolvedValue = crf
				.resolveConflict(cluster1);
		assertEquals(2, resolvedValue.getValue().size());

	}
}
