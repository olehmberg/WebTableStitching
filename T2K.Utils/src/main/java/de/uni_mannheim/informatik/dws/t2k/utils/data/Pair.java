package de.uni_mannheim.informatik.dws.t2k.utils.data;

public class Pair<T, U> {

	private T first;
	private U second;
	
	public Pair(T first, U second)
	{
		this.first = first;
		this.second = second;
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
	
	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof Pair) {
			@SuppressWarnings("unchecked")
			Pair<T,U> p = (Pair<T,U>)obj;
			return getFirst().equals(p.getFirst()) && getSecond().equals(p.getSecond());
		}
		return super.equals(obj);
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return 997 * ((int)first.hashCode()) ^ 991 * ((int)second.hashCode()); 
	}
}
