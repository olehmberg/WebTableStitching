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
package de.uni_mannheim.informatik.wdi.model;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;

import org.apache.commons.lang3.StringUtils;

import au.com.bytecode.opencsv.CSVReader;

/**
 * Represent a correspondence. Contains two {@link Record}s and their similarity
 * score.
 * 
 * @author Oliver Lehmberg (oli@dwslab.de)
 * 
 * @param <RecordType>
 */
public class Correspondence<RecordType, SchemaElementType> implements Serializable {

	private static final long serialVersionUID = 1L;
	private RecordType firstRecord;
	private RecordType secondRecord;
	private double similarityScore;
	private ResultSet<Correspondence<SchemaElementType, RecordType>> causalCorrespondences;

	/**
	 * returns the first record
	 * 
	 * @return
	 */
	public RecordType getFirstRecord() {
		return firstRecord;
	}

	/**
	 * sets the first record
	 * 
	 * @param firstRecord
	 */
	public void setFirstRecord(RecordType firstRecord) {
		this.firstRecord = firstRecord;
	}

	/**
	 * returns the second record
	 * 
	 * @return
	 */
	public RecordType getSecondRecord() {
		return secondRecord;
	}

	/**
	 * sets the second record
	 * 
	 * @param secondRecord
	 */
	public void setSecondRecord(RecordType secondRecord) {
		this.secondRecord = secondRecord;
	}

	/**
	 * returns the similarity score
	 * 
	 * @return
	 */
	public double getSimilarityScore() {
		return similarityScore;
	}

	/**
	 * sets the similarity score
	 * 
	 * @param similarityScore
	 */
	public void setsimilarityScore(double similarityScore) {
		this.similarityScore = similarityScore;
	}

	public Correspondence() {
		
	}
	
	public Correspondence(RecordType first, RecordType second,
			double similarityScore, ResultSet<Correspondence<SchemaElementType, RecordType>> correspondences) {
		firstRecord = first;
		secondRecord = second;
		this.similarityScore = similarityScore;
		this.causalCorrespondences = correspondences;
	}

	/**
	 * @return the schema correspondences that were used to calculate this correspondence
	 */
	public ResultSet<Correspondence<SchemaElementType, RecordType>> getCausalCorrespondences() {
		return causalCorrespondences;
	}
	
	/**
	 * @param causalSchemaCorrespondences the causalSchemaCorrespondences to set
	 */
	public void setCausalCorrespondences(
			ResultSet<Correspondence<SchemaElementType, RecordType>> causalCorrespondences) {
		this.causalCorrespondences = causalCorrespondences;
	}
	
	/**
	 * Combines two correspondences which have the same target schema to a correspondence between the two source schemas, i.e. a->c; b->c will be combined to a->b
	 * @param first
	 * @param second
	 * @return
	 */
	public static <RecordType, SchemaElementType> Correspondence<RecordType, SchemaElementType> combine(Correspondence<RecordType, SchemaElementType> first, Correspondence<RecordType, SchemaElementType> second) {
		ResultSet<Correspondence<SchemaElementType, RecordType>> cors = new ResultSet<>();
		cors.merge(first.getCausalCorrespondences());
		cors.merge(second.getCausalCorrespondences());
		return new Correspondence<RecordType, SchemaElementType>(first.getFirstRecord(), second.getFirstRecord(), first.getSimilarityScore() * second.getSimilarityScore(), cors);
	}
	
	/**
	 * Inverts the direction of the correspondences in the given ResultSet
	 * @param correspondences
	 */
	public static <RecordType, SchemaElementType> ResultSet<Correspondence<RecordType, SchemaElementType>> changeDirection(ResultSet<Correspondence<RecordType, SchemaElementType>> correspondences) {
		if(correspondences==null) {
			return null;
		} else {
			ResultSet<Correspondence<RecordType, SchemaElementType>> result = new ResultSet<>();
			
			for(Correspondence<RecordType, SchemaElementType> cor : correspondences.get()) {
				result.add(new Correspondence<RecordType, SchemaElementType>(cor.getSecondRecord(), cor.getFirstRecord(), cor.getSimilarityScore(), Correspondence.changeDirection(cor.getCausalCorrespondences())));
			}
			
			return result;
		}
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof Correspondence) {
			Correspondence<?,?> cor2 = (Correspondence<?,?>)obj;
			return getFirstRecord().equals(cor2.getFirstRecord()) && getSecondRecord().equals(cor2.getSecondRecord());
		} else {
			return super.equals(obj);
		}
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return 997 * (getFirstRecord().hashCode()) ^ 991 * (getSecondRecord().hashCode());
	}
	
	public static class RecordId implements Matchable {

		private String identifier;
		
		/* (non-Javadoc)
		 * @see de.uni_mannheim.informatik.wdi.model.Matchable#getIdentifier()
		 */
		@Override
		public String getIdentifier() {
			return identifier;
		}

		/**
		 * @param identifier the identifier to set
		 */
		public void setIdentifier(String identifier) {
			this.identifier = identifier;
		}
		
		/* (non-Javadoc)
		 * @see de.uni_mannheim.informatik.wdi.model.Matchable#getProvenance()
		 */
		@Override
		public String getProvenance() {
			// TODO Auto-generated method stub
			return null;
		}
		
		public RecordId(String identifier) {
			this.identifier = identifier;
		}
	}
	
	public static ResultSet<Correspondence<RecordId, RecordId>> loadFromCsv(File location) throws IOException {
		CSVReader r = new CSVReader(new FileReader(location));
		
		ResultSet<Correspondence<RecordId, RecordId>> correspondences = new ResultSet<>();
		
		String[] values = null;
		
		while((values = r.readNext())!=null) {
			if(values.length>=3) {
				String id1 = values[0];
				String id2 = values[1];
				String sim = values[2];
				Double similarityScore = 0.0;
				
				try {
					similarityScore = Double.parseDouble(sim);
				} catch(Exception ex) {
					System.err.println(ex.getMessage());
				}
				
				Correspondence<RecordId, RecordId> cor = new Correspondence<RecordId, RecordId>(new RecordId(id1), new RecordId(id2), similarityScore, null);
				correspondences.add(cor);
			} else {
				System.err.println(String.format("Invalid format: \"%s\"", StringUtils.join(values, "\",\"")));
			}
		}
		
		r.close();
		
		return correspondences;
	}
}
