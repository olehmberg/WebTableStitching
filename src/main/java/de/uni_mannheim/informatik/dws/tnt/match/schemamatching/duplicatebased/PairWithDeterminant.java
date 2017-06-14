package de.uni_mannheim.informatik.dws.tnt.match.schemamatching.duplicatebased;

import de.uni_mannheim.informatik.dws.winter.model.Pair;

public class PairWithDeterminant<T, U, DeterminantType> extends Pair<T, U> {

	private static final long serialVersionUID = 1L;
	private DeterminantType determinant;
	
	public PairWithDeterminant(T first, U second, DeterminantType determinant) {
		super(first, second);
		this.determinant = determinant;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((getFirst() == null) ? 0 : getFirst().hashCode());
		result = prime * result + ((getSecond() == null) ? 0 : getSecond().hashCode());
		result = prime * result + ((determinant == null) ? 0 : determinant.hashCode());
		return result;
	}

	@SuppressWarnings("rawtypes")
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		PairWithDeterminant other = (PairWithDeterminant) obj;
		if (getFirst() == null) {
			if (other.getFirst() != null)
				return false;
		} else if (!getFirst().equals(other.getFirst()))
			return false;
		if (getSecond() == null) {
			if (other.getSecond() != null)
				return false;
		} else if (!getSecond().equals(other.getSecond()))
			return false;
		if (determinant == null) {
			if (other.determinant != null)
				return false;
		} else if (!determinant.equals(other.determinant))
			return false;
		return true;
	}
	
	
	
}
