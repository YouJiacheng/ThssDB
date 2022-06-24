package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.*;
import cn.edu.thssdb.type.ColumnType;
import cn.edu.thssdb.common.Global;


public class Column implements Comparable<Column> {
    private final String name;
    private final ColumnType type;
    public boolean primary;
    public boolean notNull;
    public int maxLength;

    public Column(String name, ColumnType type, boolean primary, boolean notNull, int maxLength) {
        this.name = name;
        this.type = type;
        this.primary = primary;
        this.notNull = notNull;
        this.maxLength = maxLength;
    }

    @Override
    public int compareTo(Column e) {
        return name.compareTo(e.name);
    }

    public String dumps() { // for WAL
        return name + ',' + type + ',' + primary + ',' + notNull + ',' + maxLength;
    }

    public String representation() { // for pretty print
        var s = new StringBuilder();
        s.append(name);
        s.append(" ");
        s.append(type.name());
        if (type == ColumnType.STRING) {
            s.append("(");
            s.append(maxLength);
            s.append(")");
        }
        if (primary) {
            s.append(" Primary Key");
        } else if (notNull) { // primary key implies not null, don't duplicate
            s.append(" Not Null");
        }
        return s.toString();
    }

    public String getColumnName() {
        return this.name;
    }

    public ColumnType getColumnType() {
        return this.type;
    }

    public int getMaxLength() {
        return this.maxLength;
    }

    public static Column parseColumn(String s) {
        String[] sArray = s.split(",");
        return new Column(sArray[0], ColumnType.valueOf(sArray[1]), Boolean.parseBoolean(sArray[2]), Boolean.parseBoolean(sArray[3]), Integer.parseInt(sArray[4]));
    }

    public static Cell parseEntry(String s, Column column) {
        ColumnType columnType = column.getColumnType();
        if (s.equals(Global.ENTRY_NULL)) {
            if (column.notNull) throw new NullValueException(column.getColumnName());       // 该列不可为null
            else {
                Cell tmp = new Cell(Global.ENTRY_NULL);
                tmp.value = null;
                return tmp;
            }
        }
        switch (columnType) {
            case INT:
                return new Cell(Integer.valueOf(s));
            case LONG:
                return new Cell(Long.valueOf(s));
            case FLOAT:
                return new Cell(Float.valueOf(s));
            case DOUBLE:
                return new Cell(Double.valueOf(s));
            case STRING:
                String sWithoutQuotes = s.substring(1, s.length() - 1);
                if (sWithoutQuotes.length() > column.getMaxLength())                     // 长度超出该列限制
                    throw new ValueExceedException(column.getColumnName(), s.length(), column.getMaxLength(), "(when parse row)");
                return new Cell(sWithoutQuotes);
            default:
                Cell tmp = new Cell(Global.ENTRY_NULL);
                tmp.value = null;
                return tmp;
        }
    }
}
