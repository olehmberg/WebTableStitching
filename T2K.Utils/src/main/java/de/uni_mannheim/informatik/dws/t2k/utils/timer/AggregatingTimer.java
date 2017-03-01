package de.uni_mannheim.informatik.dws.t2k.utils.timer;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang.time.DurationFormatUtils;

public class AggregatingTimer
	extends Timer
{

	private AtomicLong _total = new AtomicLong();
	private AtomicInteger _count = new AtomicInteger();
	
	public AggregatingTimer(String name) {
		super(name);
	}
	
	public AggregatingTimer(String name, Timer parent) {
		super(name, parent);
	}

	protected AggregatingTimer()
	{
		
	}
	
	protected void addDuration(long duration)
	{
		//_total += duration;
	    _total.addAndGet(duration);
		//_count++;
	    _count.incrementAndGet();
	}
	
	@Override
	public void stop() {
		super.stop();
		
		addDuration(getDuration());
	}
	
	@Override
	protected String formatValue() {
		if(_total.get()==0)
		{
			return super.formatValue();
		}
		else
		{
			String value="", valueAvg="";
			
			value = DurationFormatUtils.formatDuration(_total.get(), "HH:mm:ss.S");
			valueAvg = DurationFormatUtils.formatDuration(_total.get()/(long)_count.get(), "HH:mm:ss.S");
			
			return getName() + ": " + value + "(" + _count + " times; " + valueAvg + " on avg.)";
		}
	}
}
