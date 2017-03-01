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

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;

import de.uni_mannheim.informatik.wdi.model.DefaultSchemaElement;
import de.uni_mannheim.informatik.wdi.model.Record;

/**
 * A {@link Record} representing a movie.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 * 
 */
public class Movie extends Record<DefaultSchemaElement> implements Serializable {

	/*
	 * example entry <movie> <id>academy_awards_2</id> <title>True Grit</title>
	 * <director> <name>Joel Coen and Ethan Coen</name> </director> <actors>
	 * <actor> <name>Jeff Bridges</name> </actor> <actor> <name>Hailee
	 * Steinfeld</name> </actor> </actors> <date>2010-01-01</date> </movie>
	 */

	private static final long serialVersionUID = 1L;

	public Movie(String identifier, String provenance) {
		super(identifier, provenance);
		actors = new LinkedList<>();
	}

	private String title;
	private String director;
	private DateTime date;
	private List<Actor> actors;

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	public String getDirector() {
		return director;
	}

	public void setDirector(String director) {
		this.director = director;
	}

	public DateTime getDate() {
		return date;
	}

	public void setDate(DateTime date) {
		this.date = date;
	}

	public List<Actor> getActors() {
		return actors;
	}

	public void setActors(List<Actor> actors) {
		this.actors = actors;
	}

	private Map<DefaultSchemaElement, Collection<String>> provenance = new HashMap<>();
	private Collection<String> recordProvenance;

	public void setRecordProvenance(Collection<String> provenance) {
		//this.provenance.put("RECORD", provenance);
		recordProvenance = provenance;
	}

	public Collection<String> getRecordProvenance() {
		//return provenance.get("RECORD");
		return recordProvenance;
	}

	public void setAttributeProvenance(DefaultSchemaElement attribute,
			Collection<String> provenance) {
		this.provenance.put(attribute, provenance);
	}

	public Collection<String> getAttributeProvenance(String attribute) {
		return provenance.get(attribute);
	}

	public String getMergedAttributeProvenance(DefaultSchemaElement attribute) {
		Collection<String> prov = provenance.get(attribute);

		if (prov != null) {
			return StringUtils.join(prov, "+");
		} else {
			return "";
		}
	}

//	@Override
//	public Collection<String> getAttributeNames() {
//		return Arrays.asList(new String[] { TITLE, DIRECTOR, DATE, ACTORS });
//	}

	public static final DefaultSchemaElement TITLE = new DefaultSchemaElement("Title");
	public static final DefaultSchemaElement DIRECTOR = new DefaultSchemaElement("Director");
	public static final DefaultSchemaElement DATE = new DefaultSchemaElement("Date");
	public static final DefaultSchemaElement ACTORS = new DefaultSchemaElement("Actors");
	
	@Override
	public boolean hasValue(DefaultSchemaElement attribute) {
		if(attribute==TITLE)
			return getTitle() != null && !getTitle().isEmpty();
		else if(attribute==DIRECTOR)
			return getDirector() != null && !getDirector().isEmpty();
		else if(attribute==DATE)
			return getDate() != null;
		else if(attribute==ACTORS)
			return getActors() != null && getActors().size() > 0;
		else
			return false;
	}

	@Override
	public String toString() {
		return String.format("[Movie: %s / %s / %s]", getTitle(),
				getDirector(), getDate().toString());
	}

	@Override
	public int hashCode() {
		return getIdentifier().hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		if(obj instanceof Movie){
			return this.getIdentifier().equals(((Movie) obj).getIdentifier());
		}else
			return false;
	}
	
	
	
}
