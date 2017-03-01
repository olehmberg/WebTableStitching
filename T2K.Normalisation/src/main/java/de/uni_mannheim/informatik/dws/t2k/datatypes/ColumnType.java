package de.uni_mannheim.informatik.dws.t2k.datatypes;

import de.uni_mannheim.informatik.dws.t2k.units.Unit;

/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */


/**
 *
 * @author domi
 */
public class ColumnType {
    
    private DataType type;
    private Unit unit;
    
    public ColumnType(DataType type, Unit unit) {
        this.type = type;
        this.unit = unit;
    }

    /**
     * @return the type
     */
    public DataType getType() {
        return type;
    }

    /**
     * @return the unit
     */
    public Unit getUnit() {
        return unit;
    }
    
}
