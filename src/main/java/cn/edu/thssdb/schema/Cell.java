package cn.edu.thssdb.schema;

import cn.edu.thssdb.common.Global;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

public class Cell implements Serializable {
    private static final long serialVersionUID = -5809782578272943999L;
    public Object value;

    public Cell(Object value) {
        this.value = value;
    }

    public String toString() {
        return value.toString();
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    public String getValueType() {
        if (this.value == null) return Global.ENTRY_NULL;
        String valueClassString = this.value.getClass().toString();
        if (valueClassString.contains("Integer")) return "INT";
        if (valueClassString.contains("Long")) return "LONG";
        if (valueClassString.contains("Float")) return "FLOAT";
        if (valueClassString.contains("Double")) return "DOUBLE";
        if (valueClassString.contains("String")) return "STRING";
        return "UNKNOWN";
    }

    public Cell arithmetic(Cell other, BiFunction<Double, Double, Double> op) throws Exception {
        if (value == null || other.value == null) return new Cell(null);

        if (getValueType().equals("STRING") || getValueType().equals("UNKNOWN") || other.getValueType().equals("STRING") || other.getValueType().equals("UNKNOWN"))
            throw new Exception("Arithmetic between " + getValueType() + " and " + other.getValueType() + " is not supported");

        return new Cell(op.apply(((Number) value).doubleValue(), ((Number) other.value).doubleValue()));
    }

    public boolean SQLCompareTo(Cell other, List<Integer> compareTypes) throws Exception {
        if (value == null || other.value == null) return false;
        if (getValueType().equals("UNKNOWN") || other.getValueType().equals("UNKNOWN"))
            throw new Exception("Compare between " + getValueType() + " and " + other.getValueType() + " is not supported");
        var isString = getValueType().equals("STRING");
        if (isString != other.getValueType().equals("STRING"))
            throw new Exception("Compare between " + getValueType() + " and " + other.getValueType() + " is not supported");
        if (isString) return compareTypes.contains(Integer.signum(((String) value).compareTo((String) other.value)));
        // numeric value
        return compareTypes.contains(Integer.signum(Double.compare(((Number) value).doubleValue(), ((Number) other.value).doubleValue())));
    }
}
