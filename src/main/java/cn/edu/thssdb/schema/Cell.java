package cn.edu.thssdb.schema;

import cn.edu.thssdb.common.Global;

import java.io.Serial;
import java.io.Serializable;
import java.util.List;
import java.util.function.BiFunction;

import static cn.edu.thssdb.type.ColumnType.INT;
import static cn.edu.thssdb.type.ColumnType.LONG;
import static cn.edu.thssdb.type.ColumnType.FLOAT;
import static cn.edu.thssdb.type.ColumnType.DOUBLE;
import static cn.edu.thssdb.type.ColumnType.STRING;


public class Cell implements Serializable, Comparable<Cell> {
    @Serial
    private static final long serialVersionUID = -5809782578272943999L;
    public Object value;

    public Cell(Object value) {
        this.value = value;
    }

    @Override
    public int compareTo(Cell c) {
        if (value == null || c.value == null) throw new NullPointerException();
        if (value.getClass() != c.value.getClass()) throw new ClassCastException("Different type value");
        if (value instanceof Integer) return ((Integer) value).compareTo((Integer) c.value);
        if (value instanceof Long) return ((Long) value).compareTo((Long) c.value);
        if (value instanceof Float) return ((Float) value).compareTo((Float) c.value);
        if (value instanceof Double) return ((Double) value).compareTo((Double) c.value);
        if (value instanceof String) return ((String) value).compareTo((String) c.value);
        throw new ClassCastException("Unknown type value");
    }

    public String toString() {
        if (value == null) return Global.ENTRY_NULL;
        return value.toString();
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    public String getValueType() {
        if (value == null) return Global.ENTRY_NULL;
        if (value instanceof Integer) return INT.name();
        if (value instanceof Long) return LONG.name();
        if (value instanceof Float) return FLOAT.name();
        if (value instanceof Double) return DOUBLE.name();
        if (value instanceof String) return STRING.name();
        throw new ClassCastException("Unknown type value");
    }

    public Cell arithmetic(Cell other, BiFunction<Double, Double, Double> op) throws Exception {
        if (value == null || other.value == null) return new Cell(null);

        if (value instanceof String || other.value instanceof String)
            throw new Exception("Arithmetic between " + getValueType() + " and " + other.getValueType() + " is not supported");

        return new Cell(op.apply(((Number) value).doubleValue(), ((Number) other.value).doubleValue()));
    }

    public boolean SQLCompareTo(Cell other, List<Integer> compareTypes) throws Exception {
        if (value == null || other.value == null) return false;
        var isString = value instanceof String;
        if (isString != other.value instanceof String)
            throw new Exception("Compare between " + getValueType() + " and " + other.getValueType() + " is not supported");
        if (isString) return compareTypes.contains(Integer.signum(((String) value).compareTo((String) other.value)));
        // numeric value
        return compareTypes.contains(Integer.signum(Double.compare(((Number) value).doubleValue(), ((Number) other.value).doubleValue())));
    }
}
