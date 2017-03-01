package de.uni_mannheim.informatik.dws.t2k.utils.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public abstract class TimeoutOperation<T> {

	public abstract T doOperation();
	
	public T run(long timeoutMillis)
	{
		
		ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<T> future = executor.submit(new Callable<T>() {

			public T call() throws Exception {
				return doOperation();
			}
		});

        T result = null;
        
        try {
            
            result = future.get(timeoutMillis, TimeUnit.MILLISECONDS);
            
        } catch (TimeoutException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}

        executor.shutdownNow();
        
        return result;
	}
	
	
	public static void main(String[] args) {
		TimeoutOperation<String> t = new TimeoutOperation<String>()
		{

			@Override
			public String doOperation() {
				try {
					Thread.sleep(10000);
					
				} catch (InterruptedException e) {
					//e.printStackTrace();
				}
				
				return "hello";
			}
			
		};
		
		System.out.println(t.run(100));
	}
}
