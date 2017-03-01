package de.uni_mannheim.informatik.wdi.similarity.date;

import java.util.Date;

import org.joda.time.DateTime;
import org.joda.time.Days;

import de.uni_mannheim.informatik.wdi.similarity.SimilarityMeasure;


public class NormalisedDateSimilarity extends SimilarityMeasure<DateTime> {

	private static final long serialVersionUID = 1L;
	private Date minDate = null;
    public void setMinDate(Date minDate) {
        this.minDate = minDate;
    }
    public Date getMinDate() {
        return minDate;
    }
    
    private Date maxDate = null;
    public void setMaxDate(Date maxDate) {
        this.maxDate = maxDate;
    }
    public Date getMaxDate() {
        return maxDate;
    }
    
    private int dateRange = 0;
    public int getDateRange() {
        return dateRange;
    }
    
    public void setValueRange(DateTime minValue, DateTime maxValue) {
        setMinDate(minValue.toDate());
        setMaxDate(maxValue.toDate());
        calcDateRange();
    }
    
    private void calcDateRange() {
        if(minDate!=null && maxDate!=null) {
            dateRange = Math.abs(Days.daysBetween(new DateTime(getMaxDate()), new DateTime(getMinDate())).getDays());
        }
    }
    
    @Override
    public double calculate(DateTime first, DateTime second) {
        int days = Math.abs(Days.daysBetween(first, second).getDays());
        
        return Math.max(1.0 - ((double)days / (double)getDateRange()),0.0);
    }


}
