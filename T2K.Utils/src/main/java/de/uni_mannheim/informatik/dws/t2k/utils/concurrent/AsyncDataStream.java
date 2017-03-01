package de.uni_mannheim.informatik.dws.t2k.utils.concurrent;

import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

public class AsyncDataStream<T> {

	private LinkedBlockingQueue<T> dataQueue;
	private boolean isEndOfStream;
	private boolean isInUse;
	
	public boolean isInUse() {
		return isInUse;
	}
	
	public AsyncDataStream(int maxQueueSize)
	{
		dataQueue = new LinkedBlockingQueue<T>(maxQueueSize);
		isInUse=false;
	}
	
	protected Queue<T> getDataQueue() {
		return dataQueue;
	}
	
	/**
	 * Add an element to the stream
	 * @param data
	 */
	public void put(T data)
	{
		isInUse=true;
		getDataQueue().add(data);
	}
	
	/**
	 * Get the next element from the stream or null, if the stream is currently empty
	 * @return
	 */
	public T get()
	{
		return getDataQueue().poll();
	}
	
	/**
	 * True, if this data stream is empty and there's no chance that new elements will be added
	 * @return
	 */
	public boolean isEndOfStream() {
		return isEndOfStream && getDataQueue().size() == 0;
	}
	
	/**
	 * Signals that no more elements will be added to this stream
	 */
	public void setEndOfStream() {
		this.isEndOfStream = true;
	}
	
}
