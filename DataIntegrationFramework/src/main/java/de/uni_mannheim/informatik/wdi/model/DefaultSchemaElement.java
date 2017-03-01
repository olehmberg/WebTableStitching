package de.uni_mannheim.informatik.wdi.model;

import java.io.Serializable;

public class DefaultSchemaElement implements Matchable, Serializable {

	private static final long serialVersionUID = 1L;

	public DefaultSchemaElement() {
	}
	
	public DefaultSchemaElement(String identifier) {
		id = identifier;
	}
	
	public DefaultSchemaElement(String identifier, String provenance) {
		id = identifier;
		this.provenance = provenance;
	}
	
//	private String name;
//	
//	public String getName() {
//		return name;
//	}
//	public void setName(String name) {
//		this.name = name;
//	}

	protected String id;
	protected String provenance;

	@Override
	public String getIdentifier() {
		return id;
	}

	@Override
	public String getProvenance() {
		return provenance;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return getIdentifier();
	}
}
