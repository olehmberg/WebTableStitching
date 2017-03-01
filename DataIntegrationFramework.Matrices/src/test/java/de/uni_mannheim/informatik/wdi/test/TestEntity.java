package de.uni_mannheim.informatik.wdi.test;

import java.util.Collection;
import java.util.LinkedList;

public class TestEntity extends TestInstance {

	public TestEntity(int id) {
		super(id);
		parts = new LinkedList<TestInstance>();
	}
	public TestEntity(int id, String label) {
		super(id, label);
		parts = new LinkedList<TestInstance>();
	}
	public TestEntity(int id, String label, Object type) {
		super(id, label, type);
		parts = new LinkedList<TestInstance>();
	}

	private Collection<TestInstance> parts;
	public Collection<TestInstance> getParts() {
		return parts;
	}
	public void setParts(Collection<TestInstance> parts) {
		this.parts = parts;
	}
	
}
