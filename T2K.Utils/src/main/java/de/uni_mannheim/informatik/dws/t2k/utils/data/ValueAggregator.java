package de.uni_mannheim.informatik.dws.t2k.utils.data;

public class ValueAggregator {
	double min, max, sum;
	long cnt;
	
	public ValueAggregator()
	{
		reset();
	}
	
	public void reset()
	{
		min = Double.MAX_VALUE;
		max = Double.MIN_VALUE;
		sum = 0;
		cnt = 0;
	}
	
	public void AddValue(double d)
	{
	    if(d==Double.NaN) {
	        return;
	    }
	    
		if(d<min)
			min = d;
		
		if(d>max)
			max = d;
		
		sum+=d;
		cnt++;
	}
	
	public double getMin()
	{
		if(cnt==0)
			return 0;
		else
			return min;
	}
	
	public double getMax()
	{
		if(cnt==0)
			return 0;
		else
			return max;
	}
	
	public double getAvg()
	{
		if(cnt==0)
			return 0;
		else
			return sum / (double)cnt;
	}
	
	public double getSum()
	{
		return sum;
	}
	
	public long getCount()
	{
		return cnt;
	}
}
