package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.*;
import cn.edu.thssdb.index.BPlusTree;
import cn.edu.thssdb.common.Global;
import cn.edu.thssdb.common.Pair;

import java.io.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static cn.edu.thssdb.type.ColumnType.STRING;


// TODO lock control, variables init.

public class Table implements Iterable<Row> {
    private ReentrantReadWriteLock lock;
    private String databaseName;
    public String tableName;
    public ArrayList<Column> columns;
    public BPlusTree<Cell, Row> index;
    public int primaryIndex;

    // ADD lock variables for S, X locks and etc here.

    // TODO: table/tuple level locks

    public void takeSLock(Long sessionId) {
        lock.readLock().lock();
        System.out.println("--S locked by "+ sessionId);
        System.out.println(lock.readLock().toString());
    }

    public void releaseSLock(Long sessionId) {
        System.out.println("count is "+lock.getReadHoldCount() );
        final int holdCount = lock.getReadHoldCount();
        for (int i = 0; i < holdCount; i++) {
            lock.readLock().unlock();
            System.out.println("--S release by "+ sessionId);
        }
    }

    public void takeXLock(Long sessionId) {
        lock.writeLock().lock();
        System.out.println("--X locked by "+ sessionId);
    } // 在test成功前提下拿X锁。返回值false表示session之前已拥有这个表的X锁。

    public void releaseXLock(Long sessionId) {
        lock.writeLock().unlock();
        System.out.println("--X release by "+ sessionId);
    }
    public void printLock(){
        System.out.println("=========");
        System.out.println("X lock");
        System.out.println(lock.writeLock());
        System.out.println("S lock");
        System.out.println(lock.readLock());
        System.out.println("=========");
    }


    // Initiate: Table, recover
    public Table(String databaseName, String tableName, Column[] columns) {
        this.lock = new ReentrantReadWriteLock();
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.columns = new ArrayList<>(Arrays.asList(columns));
        this.index = new BPlusTree<>();
        this.primaryIndex = -1;

        for (int i = 0; i < this.columns.size(); i++) {
            if (this.columns.get(i).primary) {
                if (this.primaryIndex >= 0) throw new MultiPrimaryKeyException(this.tableName);
                this.primaryIndex = i;
            }
        }
        if (this.primaryIndex < 0) throw new MultiPrimaryKeyException(this.tableName);

        // TODO initiate lock status.

        recover();
    }

    private void recover() {
        // read from disk for recovering
        try {
            //lock.writeLock().lock();
            ArrayList<Row> rowsOnDisk = deserialize();
            for (Row row : rowsOnDisk)
                this.index.put(row.getEntries().get(this.primaryIndex), row);
        } finally {
            //lock.writeLock().unlock();
        }
    }


    // Operations: get, insert, delete, update, dropTable, you can add other operations.
    // remember to use locks to fill the TODOs

    public Row get(Cell primaryCell) {
        try {
            //lock.readLock().lock();
            return this.index.get(primaryCell);
        } finally {
           // lock.readLock().unlock();
        }
    }

    public void insert(List<Row> rows) {
        try {
            //lock.writeLock().lock();
            checkPutValid(rows, new TreeSet<>());
            // check all, then modify for atomic
            for (var row : rows)
                index.put(row.getEntries().get(primaryIndex), row);
        } finally {
           // lock.writeLock().unlock();
        }
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

    public void delete(List<Cell> keys) {
        try {
            //lock.writeLock().lock();
            checkRemoveValid(keys);
            // check all, then modify for atomic
            for (var key : keys)
                index.remove(key);
        } finally {
            //lock.writeLock().unlock();
        }
    }

    public void update(List<Cell> oldKeys, List<Row> newRows) {
        try {
            //lock.writeLock().lock();
            checkPutValid(newRows, checkRemoveValid(oldKeys));
            // check all, then modify for atomic
            for (var key : oldKeys)
                index.remove(key);
            for (var row : newRows)
                index.put(row.getEntries().get(primaryIndex), row);
        } finally {
            //lock.writeLock().unlock();
        }
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

    public void persist() {
        try {
            //lock.readLock().lock();
            serialize();
        } finally {
            //lock.readLock().unlock();
        }
    }

    public void dropTable() { // remove table data file
        try {
            //lock.writeLock().lock();
            File tableFolder = new File(this.getTableFolderPath());
            if (!tableFolder.exists() ? !tableFolder.mkdirs() : !tableFolder.isDirectory())
                throw new FileIOException(this.getTableFolderPath() + " when dropTable");
            File tableFile = new File(this.getTablePath());
            if (tableFile.exists() && !tableFile.delete())
                throw new FileIOException(this.getTablePath() + " when dropTable");
        } finally {
           // lock.writeLock().unlock();
        }
    }


    // Operations involving logic expressions.


    // Operations

    private class TableIterator implements Iterator<Row> {
        private Iterator<Pair<Cell, Row>> iterator;

        TableIterator(Table table) {
            this.iterator = table.index.iterator();
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

    private Boolean containsRowByKey(Row row) {
        return index.contains(row.getEntries().get(primaryIndex));
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

    public String toString() {
        StringBuilder s = new StringBuilder("Table " + this.tableName + ": ");
        for (Column column : this.columns) s.append("\t(").append(column.toString()).append(')');
        return s + "\n";
    }

}
