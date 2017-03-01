package de.uni_mannheim.informatik.wdi.spark.processing;

import java.util.HashMap;

import scala.Tuple2;
import de.uni_mannheim.informatik.wdi.model.Pair;
import de.uni_mannheim.informatik.wdi.model.ResultSet;
import de.uni_mannheim.informatik.wdi.processing.GroupCollector;

public class SparkGroupCollector<KeyType, RecordType> extends GroupCollector<KeyType, RecordType> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	
	private HashMap<KeyType, ResultSet<RecordType>> groups;
	private ResultSet<Tuple2<KeyType, RecordType>> result;

	/**
	 * @return the result
	 */
	public ResultSet<Tuple2<KeyType, RecordType>> getResultData() {
		return result;
	}
	
	@Override
	public void initialise() {
		groups = new HashMap<>();
		result = new ResultSet<>();
	}

	@Override
	public void next(Pair<KeyType, RecordType> record) {
		ResultSet<RecordType> list = groups.get(record.getFirst());
		if(list==null) {
			list = new ResultSet<>();
			groups.put(record.getFirst(), list);
		}
		
		list.add(record.getSecond());
	}

	@Override
	public void finalise() {
		for(KeyType key : groups.keySet()) {
			for(RecordType r : groups.get(key).get()){
				Tuple2<KeyType, RecordType> g = new Tuple2<KeyType, RecordType>(key, r);
				result.add(g);	
			}
		}
	}

}
