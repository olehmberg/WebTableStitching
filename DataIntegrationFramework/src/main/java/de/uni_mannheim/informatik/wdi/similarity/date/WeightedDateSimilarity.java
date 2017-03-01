package de.uni_mannheim.informatik.wdi.similarity.date;

import java.util.Calendar;

import org.joda.time.DateTime;

import de.uni_mannheim.informatik.wdi.similarity.SimilarityMeasure;

public class WeightedDateSimilarity extends SimilarityMeasure<DateTime> {

	private static final long serialVersionUID = 1L;
	private double dayWeight;
    public double getDayWeight() {
        return dayWeight;
    }
    public void setDayWeight(double dayWeight) {
        this.dayWeight = dayWeight;
    }
    
    private double monthWeight;
    public double getMonthWeight() {
        return monthWeight;
    }
    public void setMonthWeight(double monthWeight) {
        this.monthWeight = monthWeight;
    }
    
    private double yearWeight;
    public double getYearWeight() {
        return yearWeight;
    }
    public void setYearWeight(double yearWeight) {
        this.yearWeight = yearWeight;
    }
    
    private int yearRange = 0;
    public int getYearRange() {
		return yearRange;
	}
    public void setYearRange(int yearRange) {
		this.yearRange = yearRange;
	}
    
    public WeightedDateSimilarity() {
        
    }
    
    public WeightedDateSimilarity(double dayWeight, double monthWeight, double yearWeight) {
        this.dayWeight = dayWeight;
        this.monthWeight = monthWeight;
        this.yearWeight = yearWeight;
    }
    
	@Override
	public double calculate(DateTime first, DateTime second) {
		if(first==null || second==null) {
			return 0.0;
		}
		
        Calendar calFirst = Calendar.getInstance();
        calFirst.setTime(first.toDate());
        Calendar calSecond = Calendar.getInstance();
        calSecond.setTime(second.toDate());
        
        if(calFirst.get(Calendar.DAY_OF_YEAR) == 1 || calSecond.get(Calendar.DAY_OF_YEAR) == 1) {
            double yearSim = 1.0 - ((double)Math.abs(calFirst.get(Calendar.YEAR) - calSecond.get(Calendar.YEAR)) / (double)Math.abs(yearRange));
            return Math.max(yearSim, 0.0);
        }
        
        int days = Math.abs(calFirst.get(Calendar.DAY_OF_MONTH) - calSecond.get(Calendar.DAY_OF_MONTH));
        int months = Math.abs(calFirst.get(Calendar.MONTH) - calSecond.get(Calendar.MONTH));
        
        double daySim = (31.0 - days) / 31.0;
        double monthSim = (12.0 - months) / 12.0;
        double yearSim = 1.0 - ((double)Math.abs(calFirst.get(Calendar.YEAR) - calSecond.get(Calendar.YEAR)) / (double)Math.abs(yearRange));
        
        if(yearSim<0.0) {
        	// outside of year range
        	return 0.0;
        } else {
	        daySim = getDayWeight()*daySim;
	        monthSim = getMonthWeight()*monthSim;
	        yearSim = getYearWeight()*yearSim;
	        double value = daySim + monthSim + yearSim;
	        value = value/(getDayWeight()+getMonthWeight()+getYearWeight());
	        return value;
        }
	}

}
