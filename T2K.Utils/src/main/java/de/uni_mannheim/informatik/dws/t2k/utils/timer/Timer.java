package de.uni_mannheim.informatik.dws.t2k.utils.timer;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang.time.DurationFormatUtils;

public class Timer {

	protected Long _start;
	private Long _end;
	private String _name;

	private static Timer _current;
	private Timer _parent;
	private boolean silent;
	
	
	private static boolean verbose = false;
	
	/**
	 * Static property defining the verbosity (system.out) for ALL timers
	 * @param verbose
	 */
	public static void setVerbose(boolean verbose) {
	    Timer.verbose = verbose;
	}
	public static boolean isVerbose() {
	    return verbose;
	}
	
	public boolean isSilent() {
        return silent;
    }
	/**
	 * Overrides verbosity for individual timers
	 * @param silent
	 */
	public void setSilent(boolean silent) {
        this.silent = silent;
    }

	private List<Timer> _children = new LinkedList<Timer>();
	private static HashMap<String, Timer> _named = new HashMap<String, Timer>();
	
	//private static java.util.concurrent.locks.ReentrantLock createTimerLock = new ReentrantLock();
	private static Object createTimerLock = new Object();

	public List<Timer> getChildren() {
		return _children;
	}

	protected void addChild(Timer child) {
		_children.add(child);
	}

	protected boolean hasChild(Timer child) {
		return _children.contains(child);
	}

	public Timer(String name) {
		this(name, null);
	}

	public Timer(String name, Timer parent) {
		_name = name;

		_named.put(name, this);

		if (parent == null)
			_parent = _current;
		else
			_parent = parent;
		if (_parent != null && !_parent.hasChild(this))
			_parent.addChild(this);
		_current = this;

		start();
	}

	protected void setCurrent(Timer current) {
		_current = current;
	}

	protected Timer() {

	}

	public static Timer getNamed(String name, Timer parent) {
	    return getNamed(name, parent, false);
	}
	
	public static Timer getNamed(String name, Timer parent, boolean isSilent) {
	    Timer result = null;
	    
		//createTimerLock.lock();
	    synchronized (createTimerLock) {

	        AggregatingTimer t = null;
	        
    		if (!_named.containsKey(name)) {
    			// retrieving a timer that was created previously only makes sense
    			// for aggregated timers
    			// so if we create a new timer, it is an AggregatingTimer
    
    			t = new AggregatingTimer(name, parent);
    			_named.put(name, t);
    		} else {
        		t = (AggregatingTimer) _named.get(name);
        
        		t.setSilent(isSilent);
        		
        		t.start();
    		}

            // assume this is only used during multi-threading, so return a
            // decorated timer that can handle this
    		result = new MultiTimer(t);
		//createTimerLock.unlock();
	    }

		return result;
	}

	protected void start() {
		_start = System.currentTimeMillis();
		
		if(!isSilent()) {
		    if(isVerbose()) {
		        System.out.println(printStart());
		    }
		}
	}

	protected void setEnd() {
		_end = System.currentTimeMillis();
	}

	public void stop() {
		setEnd();

		_current = _parent;
		
		if(!isSilent()) {
		    if(isVerbose()) {
		        System.out.println(printEnd());
		    }
		}
	}

	@Override
	public String toString() {
		return this.print("").toString();
	}

	public long getDuration() {
		if (_end == null)
			return 0;
		return _end - _start;
	}

	public String getName() {
		return _name;
	}

	protected StringBuilder print(String prefix) {
		StringBuilder sb = new StringBuilder();

		sb.append(prefix + formatValue() + "\n");

		for (Timer t : getChildren())
			sb.append(prefix + t.print(prefix + " "));

		return sb;
	}
	
	public String printStart() {
	    return "[Started] " + getName();
	}
	
	public String printEnd() {
	    return "[Stopped] " + formatValue();
	}

	protected String formatValue() {
		String value = "";

		if (_end == null) {
			long dur = System.currentTimeMillis() - _start;
			value = " still running ("
					+ DurationFormatUtils.formatDuration(dur, "HH:mm:ss.S")
					+ " so far)";
		} else {
			long dur = _end - _start;
			value = DurationFormatUtils.formatDuration(dur, "HH:mm:ss.S");
		}

		return this._name + ": " + value;
	}
}
