package de.uni_mannheim.informatik.dws.t2k.utils.concurrent;

import java.util.concurrent.ThreadPoolExecutor;

import de.uni_mannheim.informatik.dws.t2k.utils.concurrent.Consumer;

public abstract class Producer<T> {

	public abstract void execute();
	
	private ThreadPoolExecutor pool;
	private Consumer<T> consumer;
	protected boolean runsSingleThreaded = false;
	protected void setRunSingleThreaded(boolean singleThreaded) {
	    runsSingleThreaded = singleThreaded;
	}
	protected boolean isSingleThreaded() {
	    return runsSingleThreaded;
	}
	
	public void setPool(ThreadPoolExecutor pool)
	{
		this.pool = pool;
	}
	
	public void setConsumer(Consumer<T> consumer)
	{
		this.consumer = consumer;
	}
	
	protected void produce(final T value)
	{
	    if(!runsSingleThreaded) {
    		pool.execute(new Runnable() {
    			
    			public void run() {
    				consumer.execute(value);
    			}
    		});
	    } else {
	        consumer.execute(value);
	    }
	}
	
}
