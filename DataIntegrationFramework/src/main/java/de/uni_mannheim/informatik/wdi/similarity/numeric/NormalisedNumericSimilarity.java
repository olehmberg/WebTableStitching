package de.uni_mannheim.informatik.wdi.similarity.numeric;

import de.uni_mannheim.informatik.wdi.similarity.SimilarityMeasure;



public class NormalisedNumericSimilarity extends SimilarityMeasure<Double> {

	private static final long serialVersionUID = 1L;
	private Double min;
    public Double getMin() {
        return min;
    }
    public void setMin(Double min) {
        this.min = min;
    }
    
    private Double max;
    public Double getMax() {
        return max;
    }
    public void setMax(Double max) {
        this.max = max;
    }
    
    private Double range;
    public Double getRange() {
        return range;
    }
    public void setRange(Double range) {
        this.range = range;
    }
    
    public void setValueRange(Double minValue, Double maxValue) {
        setMax(maxValue);
        setMin(minValue);
        
        if(getMin()!=null && getMax()!=null) {
            setRange(getMax() - getMin());
        }
    }
    
    @Override
    public double calculate(Double first, Double second) {
        
        Double diff = Math.abs(first-second);
        
        if(diff>range) {
            return 0.0;
        } else {
            return diff / range;
        }
    }

}
