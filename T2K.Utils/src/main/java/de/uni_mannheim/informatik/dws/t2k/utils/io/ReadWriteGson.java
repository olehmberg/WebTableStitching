package de.uni_mannheim.informatik.dws.t2k.utils.io;



import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.commons.io.FileUtils;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

/**
 * @author annalisa
 *
 * @param <T>
 * small class to read write a class from json, using Gson library
 */
public class ReadWriteGson<T> {

	T obj;

	public ReadWriteGson(T obj) {
		super();
		this.obj = obj;
	}


	@SuppressWarnings("unchecked")
	public T fromJson(File file) throws JsonSyntaxException, IOException {
		Gson gson = new Gson();
		return (T) gson.fromJson(FileUtils.readFileToString(file),
				obj.getClass());
	}

	public void writeJson(File file) throws IOException {
		Gson gson = new Gson();
		BufferedWriter w = new BufferedWriter(new FileWriter(file));
		w.write(gson.toJson(obj));
		w.close();
	}

}
