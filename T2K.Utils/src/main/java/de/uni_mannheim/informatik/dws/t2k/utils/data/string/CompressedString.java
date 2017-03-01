package de.uni_mannheim.informatik.dws.t2k.utils.data.string;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;

public class CompressedString {
	
	private static byte[] allTheStrings = null;
	private static ByteArrayOutputStream stringStream = new ByteArrayOutputStream();
	private static HashMap<String, CompressedString> stringCache = new HashMap<>();
	
	private int offset;
	private int length;
	
	private CompressedString(int offset, int length) {
		this.offset = offset;
		this.length = length;
	}
	
	@Override
	public String toString() {
		try {
			return new String(allTheStrings, offset, length, "UTF-8");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
	
	public static CompressedString compress(String s) throws IOException {
		if(s==null) {
			return null;
		} else {
			CompressedString s2 = stringCache.get(s);
			if(s2==null) {
				byte[] data = s.getBytes("UTF-8");
				s2 = new CompressedString(stringStream.size(), data.length);
				stringStream.write(data);
				stringCache.put(s, s2);
				return s2;
			} else {
				return s2;
			}
		}
	}
	
	public static void materialiseCompressedStrings() {
		allTheStrings = stringStream.toByteArray();
	}
	
	public static void freeOverhead() {
		stringStream = null;
		stringCache = null;
	}
}
