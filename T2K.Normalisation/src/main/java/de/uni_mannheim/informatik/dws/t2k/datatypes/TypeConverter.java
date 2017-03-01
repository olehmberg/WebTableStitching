package de.uni_mannheim.informatik.dws.t2k.datatypes;

import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Locale;

import org.joda.time.DateTime;

import de.uni_mannheim.informatik.dws.t2k.units.Unit;
import de.uni_mannheim.informatik.dws.t2k.units.UnitParser;

public class TypeConverter {

	private boolean verbose = false;
	public boolean isVerbose() {
		return verbose;
	}
	public void setVerbose(boolean verbose) {
		this.verbose = verbose;
	}
	
	/**
	 * Converts a String into the given type
	 * @param Value
	 * @param type
	 * @param unit
	 * @return
	 * @throws ParseException
	 */
    public Object typeValue(String value, DataType type, Unit unit) {
        Object typedValue = null;
        
        if(value!=null) {
	        try {
		        switch (type) {
		            case string:
		                typedValue = value;
		                break;
		            case date:
		                typedValue = new DateTime(DateUtil.parse(value));
		                break;
		            case numeric:
		                //TODO: how to handle numbers with commas (German style)
		                if (unit != null) {
		                    typedValue = UnitParser.transformUnit(value, unit);
		
		                } else {
		                    value = value.replaceAll("[^0-9\\,\\.\\-Ee\\+]", "");
		                    NumberFormat format = NumberFormat.getInstance(Locale.US);
		                    Number number = format.parse(value);
		                    typedValue = number.doubleValue();
		                }
		                break;
		            case bool:
		                typedValue = Boolean.parseBoolean(value);
		                break;
		            case coordinate:
		                typedValue = value;
		                break;
		            case link:
		                typedValue = value;
		            default:
		                break;
		        }
	        } catch(ParseException e) {
	        	if(isVerbose()) {
	        		e.printStackTrace();
	        	}
	        }
        }
        
        return typedValue;
    }
	
}
