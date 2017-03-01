package de.uni_mannheim.informatik.dws.t2k.utils.timer;

import java.util.List;

/**
 * Used during Multi-Threading
 * @author Oliver
 *
 */
public class MultiTimer
	extends AggregatingTimer
{

	private AggregatingTimer _object;
	
	public MultiTimer(AggregatingTimer timer) {
		_object = timer;
		start();
		
		setCurrent(_object);
	}
	
	@Override
	public List<Timer> getChildren() {
		return _object.getChildren();
	}
	
	@Override
	protected void addChild(Timer child) {
		_object.addChild(child);
	}
	
	@Override
	protected boolean hasChild(Timer child) {
		return _object.hasChild(child);
	}
	
	@Override
	protected void start() {
        _start = System.currentTimeMillis();
	}
	
	@Override
	public void stop() {
	    synchronized (_object) {
	        setEnd();
	        _object.addDuration(super.getDuration());
	        
	        if(!_object.isSilent()) {
	            if(isVerbose()) {
	                System.out.println(_object.printEnd());
	            }
	        }
        }
	}
	
	@Override
	public String getName() {
		return _object.getName();
	}
	
	@Override
	protected StringBuilder print(String prefix) {
		return _object.print(prefix);
	}
	
	@Override
	protected void addDuration(long duration) {
		synchronized (_object) {
		    _object.addDuration(duration);
        }
	}
	
	@Override
	protected String formatValue() {
		 return _object.formatValue();
	}
	
	@Override
	public long getDuration() {
		return _object.getDuration();
	}
	
	@Override
	public boolean equals(Object obj) {
		return _object.equals(obj);
	}
	
	@Override
	public int hashCode() {
		return _object.hashCode();
	}
	
	@Override
	public String toString() {
		return _object.toString();
	}
}
