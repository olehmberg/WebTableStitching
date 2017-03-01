/** 
 *
 * Copyright (C) 2015 Data and Web Science Group, University of Mannheim, Germany (code@dwslab.de)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 		http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package de.uni_mannheim.informatik.wdi.usecase.movies.model;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.w3c.dom.Node;

import de.uni_mannheim.informatik.wdi.model.DefaultSchemaElement;
import de.uni_mannheim.informatik.wdi.model.FusableFactory;
import de.uni_mannheim.informatik.wdi.model.MatchableFactory;
import de.uni_mannheim.informatik.wdi.model.RecordGroup;

/**
 * A {@link MatchableFactory} for {@link Movie}s.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 * 
 */
public class MovieFactory extends MatchableFactory<Movie> implements
		FusableFactory<Movie, DefaultSchemaElement> {

	@Override
	public Movie createModelFromElement(Node node, String provenanceInfo) {
		String id = getValueFromChildElement(node, "id");

		// create the object with id and provenance information
		Movie movie = new Movie(id, provenanceInfo);

		// fill the attributes
		movie.setTitle(getValueFromChildElement(node, "title"));
		movie.setDirector(getValueFromChildElement(node, "director"));

		// convert the date string into a DateTime object
		try {
			String date = getValueFromChildElement(node, "date");
			if (date != null && !date.isEmpty()) {
				DateTime dt = DateTime.parse(date);
				movie.setDate(dt);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

		// load the list of actors
		List<Actor> actors = getObjectListFromChildElement(node, "actors",
				"actor", new ActorFactory(), provenanceInfo);
		movie.setActors(actors);

		return movie;
	}

	@Override
	public Movie createInstanceForFusion(RecordGroup<Movie, DefaultSchemaElement> cluster) {

		List<String> ids = new LinkedList<>();

		for (Movie m : cluster.getRecords()) {
			ids.add(m.getIdentifier());
		}

		Collections.sort(ids);

		String mergedId = StringUtils.join(ids, '+');

		return new Movie(mergedId, "fused");
	}

}
