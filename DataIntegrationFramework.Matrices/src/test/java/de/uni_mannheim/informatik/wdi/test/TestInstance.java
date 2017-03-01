package de.uni_mannheim.informatik.wdi.test;

import java.util.Collection;

public class TestInstance implements Comparable<TestInstance> {
	private int id;
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	private String label;
	public String getLabel() {
		return label;
	}
	public void setLabel(String label) {
		this.label = label;
	}
	private Object type;
	public Object getType() {
		return type;
	}
	public void setType(Object type) {
		this.type = type;
	}
	public TestInstance(int id) {
		this.id = id;
	}
	public TestInstance(int id, String label) {
		this.id = id;
		this.label = label;
	}
	public TestInstance(int id, String label, Object type) {
		this.id = id;
		this.label = label;
		this.type = type;
	}
    public int compareTo(TestInstance arg0) {
        return Integer.compare(id, arg0.getId());
    }
    
    private Collection<String> list;
    public Collection<String> getList() {
        return list;
    }
    public void setList(Collection<String> list) {
        this.list = list;
    }
}
