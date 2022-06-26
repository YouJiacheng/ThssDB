package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.*;
import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.common.Global;
import cn.edu.thssdb.common.Pair;

import java.io.*;
import java.util.*;

import static cn.edu.thssdb.type.ColumnType.STRING;


// Lock control logic is written in SQLHandler.evaluate
public class Table implements Iterable<Row> {
    private final String databaseName;
    public String tableName;
    public ArrayList<Column> columns;
    public BPlusTree<Cell, Row> index;
    public int primaryIndex;
    public SessionLock lock;

    // Initiate: Table, recover
    public Table(String databaseName, String tableName, List<Column> columns) {
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.columns = new ArrayList<>(columns);
        this.index = new BPlusTree<>();
        this.primaryIndex = -1;
        lock = new SessionLock();
        for (int i = 0; i < this.columns.size(); i++) {
            if (this.columns.get(i).primary) {
                if (this.primaryIndex >= 0) throw new MultiPrimaryKeyException(this.tableName);
                this.primaryIndex = i;
            }
        }
        if (this.primaryIndex < 0) throw new MultiPrimaryKeyException(this.tableName);
        ArrayList<Row> rowsOnDisk = deserialize();
        for (Row row : rowsOnDisk)
            index.put(row.getEntries().get(primaryIndex), row);
    }


    // Operations: get, insert, delete, update, dropTable, you can add other operations.
    // lock in SQLHandler.evaluate

    public Row get(Cell primaryCell) {
        return this.index.get(primaryCell);
    }

    public void insert(List<Row> rows) {
        checkPutValid(rows, new TreeSet<>());
        // check all, then modify for atomic
        for (var row : rows)
            index.put(row.getEntries().get(primaryIndex), row);
    }

    public void delete(List<Cell> keys) {
        checkRemoveValid(keys);
        // check all, then modify for atomic
        for (var key : keys)
            index.remove(key);
    }

    public void update(List<Cell> oldKeys, List<Row> newRows) {
        checkPutValid(newRows, checkRemoveValid(oldKeys));
        // check all, then modify for atomic
        for (var key : oldKeys)
            index.remove(key);
        for (var row : newRows)
            index.put(row.getEntries().get(primaryIndex), row);
    }

    private void checkPutValid(List<Row> rows, TreeSet<Cell> removed) {
        var keySet = new TreeSet<Cell>();
        for (var row : rows) {
            checkRowValidInTable(row);
            var key = row.getEntries().get(primaryIndex);
            // for update, removed key can be re-put
            if (index.contains(key) && !removed.contains(key)) throw new DuplicateKeyException();
            if (keySet.contains(key)) throw new DuplicateKeyException();
            keySet.add(key);
        }
    }

    private TreeSet<Cell> checkRemoveValid(List<Cell> keys) {
        var keySet = new TreeSet<>(keys); // remove multiple times will cause KeyNotExistException
        if (keySet.size() < keys.size()) throw new KeyNotExistException();
        for (var key : keys)
            if (!index.contains(key)) throw new KeyNotExistException();
        return keySet;
    }

    private void serialize() {
        try {
            File tableFolder = new File(this.getTableFolderPath());
            if (!tableFolder.exists() ? !tableFolder.mkdirs() : !tableFolder.isDirectory())
                throw new FileIOException(this.getTableFolderPath() + " on serializing table in folder");
            File tableFile = new File(this.getTablePath());
            if (!tableFile.exists() ? !tableFile.createNewFile() : !tableFile.isFile())
                throw new FileIOException(this.getTablePath() + " on serializing table to file");
            FileOutputStream fileOutputStream = new FileOutputStream(this.getTablePath());
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(fileOutputStream);
            for (Row row : this)
                objectOutputStream.writeObject(row);
            objectOutputStream.close();
            fileOutputStream.close();
        } catch (IOException e) {
            throw new FileIOException(this.getTablePath() + " on serializing");
        }
    }

    private ArrayList<Row> deserialize() {
        try {
            File tableFolder = new File(this.getTableFolderPath());
            if (!tableFolder.exists() ? !tableFolder.mkdirs() : !tableFolder.isDirectory())
                throw new FileIOException(this.getTableFolderPath() + " when deserialize");
            File tableFile = new File(this.getTablePath());
            if (!tableFile.exists()) return new ArrayList<>();
            FileInputStream fileInputStream = new FileInputStream(this.getTablePath());
            ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
            ArrayList<Row> rowsOnDisk = new ArrayList<>();
            Object tmpObj;
            while (fileInputStream.available() > 0) {
                tmpObj = objectInputStream.readObject();
                rowsOnDisk.add((Row) tmpObj);
            }
            objectInputStream.close();
            fileInputStream.close();
            return rowsOnDisk;
        } catch (IOException e) {
            throw new FileIOException(this.getTablePath() + " when deserialize");
        } catch (ClassNotFoundException e) {
            throw new FileIOException(this.getTablePath() + " when deserialize(serialized object cannot be found)");
        }
    }

    public synchronized void persist() {
        serialize();
    }

    public void dropTable() { // remove table data file
        File tableFolder = new File(this.getTableFolderPath());
        if (!tableFolder.exists() ? !tableFolder.mkdirs() : !tableFolder.isDirectory())
            throw new FileIOException(this.getTableFolderPath() + " when dropTable");
        File tableFile = new File(this.getTablePath());
        if (tableFile.exists() && !tableFile.delete())
            throw new FileIOException(this.getTablePath() + " when dropTable");
    }


    // Operations involving logic expressions.


    // Operations

    private static class TableIterator implements Iterator<Row> {
        private final Iterator<Pair<Cell, Row>> iterator;

        TableIterator(Table table) {
            iterator = table.index.iterator();
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Row next() {
            return iterator.next().right;
        }
    }

    @Override
    public Iterator<Row> iterator() {
        return new TableIterator(this);
    }

    private void checkRowValidInTable(Row row) {
        if (row.getEntries().size() != this.columns.size())
            throw new SchemaLengthMismatchException(this.columns.size(), row.getEntries().size(), "when check Row Valid In table");
        for (int i = 0; i < row.getEntries().size(); i++) {
            String entryValueType = row.getEntries().get(i).getValueType();
            Column column = this.columns.get(i);
            if (entryValueType.equals(Global.ENTRY_NULL)) {
                if (column.notNull) throw new NullValueException(column.getColumnName());
            } else {
                if (!entryValueType.equals(column.getColumnType().name()))
                    throw new ValueFormatInvalidException("(when check row valid in table)");
                Object entryValue = row.getEntries().get(i).value;
                if (entryValueType.equals(STRING.name()) && ((String) entryValue).length() > column.getMaxLength())
                    throw new ValueExceedException(column.getColumnName(), ((String) entryValue).length(), column.getMaxLength(), "(when check row valid in table)");
            }
        }
    }

    public String getTableFolderPath() {
        return Global.DBMS_DIR + File.separator + "data" + File.separator + databaseName + File.separator + "tables";
    }

    public String getTablePath() {
        return this.getTableFolderPath() + File.separator + this.tableName;
    }

    public String getTableMetaPath() {
        return this.getTablePath() + Global.META_SUFFIX;
    }

    @Override
    public String toString() {
        StringBuilder s = new StringBuilder("Table " + this.tableName + ": ");
        for (var column : columns) s.append("\t(").append(column.toString()).append(')');
        return s + "\n";
    }

}
