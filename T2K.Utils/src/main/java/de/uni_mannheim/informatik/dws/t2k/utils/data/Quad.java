package de.uni_mannheim.informatik.dws.t2k.utils.data;

public class Quad<T, U, V, W> {

	private T first;
	private U second;
	private V third;
	private W fourth;
	
	public Quad(T first, U second, V third, W fourth) {
		this.first = first;
		this.second = second;
		this.third = third;
		this.fourth = fourth;
	}
	
	public T getFirst() {
		return first;
	}
	
	public void setFirst(T first) {
		this.first = first;
	}
	
	public U getSecond() {
		return second;
	}
	
	public void setSecond(U second) {
		this.second = second;
	}
	
	public V getThird() {
		return third;
	}
	
	public void setThird(V third) {
		this.third = third;
	}
	
	public W getFourth() {
		return fourth;
	}
	
	public void setFourth(W fourth) {
		this.fourth = fourth;
	}
}
