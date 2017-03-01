package de.uni_mannheim.informatik.dws.t2k.utils.io;

public abstract class DataStorageStringString extends DataStorageUtils2<String, String> {

	@Override
	public String createValueFromString1(String value) {
		return value;
	}

	@Override
	public String createValueFromString2(String value) {
		return value;
	}

}
