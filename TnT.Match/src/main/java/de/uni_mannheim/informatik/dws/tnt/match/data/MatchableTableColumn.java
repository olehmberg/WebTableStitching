package de.uni_mannheim.informatik.dws.tnt.match.data;

import java.util.Comparator;

import de.uni_mannheim.informatik.dws.t2k.datatypes.DataType;
import de.uni_mannheim.informatik.dws.t2k.utils.query.Func;
import de.uni_mannheim.informatik.dws.t2k.webtables.TableColumn;
import de.uni_mannheim.informatik.wdi.model.Fusable;
import de.uni_mannheim.informatik.wdi.model.Matchable;

public class MatchableTableColumn implements Matchable, Fusable<MatchableTableColumn>, Comparable<MatchableTableColumn> {

	public static class ColumnIdProjection implements Func<String, MatchableTableColumn> {

		/* (non-Javadoc)
		 * @see de.uni_mannheim.informatik.dws.t2k.utils.query.Func#invoke(java.lang.Object)
		 */
		@Override
		public String invoke(MatchableTableColumn in) {
			return in.getIdentifier();
		}
		
	}
	
	public static class ColumnIndexProjection implements Func<Integer, MatchableTableColumn> {

		/* (non-Javadoc)
		 * @see de.uni_mannheim.informatik.dws.t2k.utils.query.Func#invoke(java.lang.Object)
		 */
		@Override
		public Integer invoke(MatchableTableColumn in) {
			return in.getColumnIndex();
		}
		
	}
	
	public static class ColumnHeaderProjection implements Func<String, MatchableTableColumn> {

		/* (non-Javadoc)
		 * @see de.uni_mannheim.informatik.dws.t2k.utils.query.Func#invoke(java.lang.Object)
		 */
		@Override
		public String invoke(MatchableTableColumn in) {
			return in.getHeader();
		}
		
	}
	
	public static class IsStringColumnProjection implements Func<Boolean, MatchableTableColumn> {

		/* (non-Javadoc)
		 * @see de.uni_mannheim.informatik.dws.t2k.utils.query.Func#invoke(java.lang.Object)
		 */
		@Override
		public Boolean invoke(MatchableTableColumn in) {
			return in.getType()==DataType.string;
		}
		
	}
	
	public static class ColumnIndexComparator implements Comparator<MatchableTableColumn> {

		/* (non-Javadoc)
		 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
		 */
		@Override
		public int compare(MatchableTableColumn o1, MatchableTableColumn o2) {
			return Integer.compare(o1.getColumnIndex(), o2.getColumnIndex());
		}
		
	}
	
	public static class TableIdColumnIndexComparator implements Comparator<MatchableTableColumn> {

		/* (non-Javadoc)
		 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
		 */
		@Override
		public int compare(MatchableTableColumn o1, MatchableTableColumn o2) {
			int result = Integer.compare(o1.getTableId(), o2.getTableId());
			
			if(result!=0) {
				return result;
			}
			
			return Integer.compare(o1.getColumnIndex(), o2.getColumnIndex());
		}
		
	}
	
	protected int tableId;
	protected int columnIndex;
	protected String id;
	protected DataType type;
	protected String header;
//	private boolean isKey = false;
	
	/**
	 * @return the header
	 */
	public String getHeader() {
		return header;
	}
	
	public int getTableId() {
		return tableId;
	}
	public int getColumnIndex() {
		return columnIndex;
	}
//	
//	public static final int CSV_LENGTH = 4;
//	
//	public static MatchableTableColumn fromCSV(String[] values, Map<String, Integer> tableIndices) {
//		MatchableTableColumn c = new MatchableTableColumn();
//		c.tableId = tableIndices.get(values[0]);
//		c.columnIndex = Integer.parseInt(values[1]);
//		c.type = DataType.valueOf(values[2]);
//		c.id = values[3];
//		return c;
//	}
	
	public MatchableTableColumn() {
		
	}
	
	public MatchableTableColumn(String identifier) {
		this.id = identifier;
	}
	
	public MatchableTableColumn(int tableId, TableColumn c) {
		this.tableId = tableId;
		this.columnIndex = c.getColumnIndex();
		this.type = c.getDataType();
//		this.isKey = c.getTable().getKey()!=null && c.getTable().getKey().equals(c);
		this.header = c.getHeader();
		
		// this controls the schema that we are matching to!
		// using c.getIdentifier() all dbp properties only exist once! (i.e. we cannot handle "_label" columns and the value of tableId is more or less random
		this.id = c.getUniqueName();
	}
	
	@Override
	public String getIdentifier() {
		return id;
	}

	@Override
	public String getProvenance() {
		return null;
	}

	/**
	 * @return the type
	 */
	public DataType getType() {
		return type;
	}
	/* (non-Javadoc)
	 * @see de.uni_mannheim.informatik.wdi.model.Fusable#hasValue(java.lang.Object)
	 */
	@Override
	public boolean hasValue(MatchableTableColumn attribute) {
		// TODO Auto-generated method stub
		return false;
	}
	/* (non-Javadoc)
	 * @see java.lang.Comparable#compareTo(java.lang.Object)
	 */
	@Override
	public int compareTo(MatchableTableColumn o) {
		return getIdentifier().compareTo(o.getIdentifier());
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#hashCode()
	 */
	@Override
	public int hashCode() {
		return getIdentifier().hashCode();
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#equals(java.lang.Object)
	 */
	@Override
	public boolean equals(Object obj) {
		if(obj instanceof MatchableTableColumn) {
			MatchableTableColumn col = (MatchableTableColumn)obj;
			return getIdentifier().equals(col.getIdentifier());
		} else {
			return super.equals(obj);
		}
	}
	
//	public boolean isKey() {
//		return isKey;
//	}
	
	/* (non-Javadoc)
	 * @see java.lang.Object#toString()
	 */
	@Override
	public String toString() {
		return String.format("{#%d}[%d]%s", getTableId(), getColumnIndex(), getHeader());
	}
}
