package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DuplicateTableException;
import cn.edu.thssdb.exception.FileIOException;
import cn.edu.thssdb.exception.TableNotExistException;
import cn.edu.thssdb.common.Global;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;


public class Database {

    private final String databaseName;
    private HashMap<String, Table> tableMap;
    SessionLock lock;

    public Database(String databaseName) {
        this.databaseName = databaseName;
        tableMap = new HashMap<>();
        lock = new SessionLock();
        File tableFolder = new File(getDatabaseTableFolderPath());
        if (!(tableFolder.exists() || tableFolder.mkdirs())) throw new RuntimeException("Create folder fail");
        recover();
    }


    // Operations: (basic) persist, create tables
    public synchronized void persistMeta() {
        // 把各表的元数据写到磁盘上
        for (var table : tableMap.values()) {
            var filename = table.getTableMetaPath();
            var columns = table.columns;
            try {
                FileOutputStream fileOutputStream = new FileOutputStream(filename);
                OutputStreamWriter outputStreamWriter = new OutputStreamWriter(fileOutputStream);
                for (var column : columns)
                    outputStreamWriter.write(column.toString() + "\n");
                outputStreamWriter.close();
                fileOutputStream.close();
            } catch (Exception e) {
                throw new FileIOException(filename);
            }
        }
    }

    public synchronized void persistTable() {
        for (var table : tableMap.values())
            table.persist();
    }

    // lock control in SQLHandler.evaluate, lock database
    public synchronized void create(String tableName, List<Column> columns) {
        if (tableMap.containsKey(tableName)) throw new DuplicateTableException(tableName);
        tableMap.put(tableName, new Table(databaseName, tableName, columns));
        persistMeta();
    }

    // NO Lock Needed
    public synchronized Table get(String tableName) {
        if (!tableMap.containsKey(tableName)) throw new TableNotExistException(tableName);
        return tableMap.get(tableName);
    }

    // lock control in SQLHandler.evaluate, lock database and table
    public synchronized void drop(String tableName) {
        var table = get(tableName);
        String filename = table.getTableMetaPath();
        File file = new File(filename);
        if (file.isFile() && !file.delete())
            throw new FileIOException(tableName + " _meta  when drop a table in database");
        table.dropTable();
        tableMap.remove(tableName);
    }

    // lock control in Manager.deleteDatabase
    public synchronized void dropDatabase() {
        for (var table : tableMap.values()) {
            File file = new File(table.getTableMetaPath());
            if (file.isFile() && !file.delete())
                throw new FileIOException(this.databaseName + " _meta when drop the database");
            table.dropTable();
        }
        tableMap.clear();
        tableMap = null;
    }

    private synchronized void recover() {
        System.out.println("! try to recover database " + databaseName + " from disk");
        File tableFolder = new File(this.getDatabaseTableFolderPath());
        File[] files = tableFolder.listFiles();
        if (files == null) return;

        for (File file : files) {
            if (!file.isFile() || !file.getName().endsWith(Global.META_SUFFIX)) continue;
            try {
                String fileName = file.getName();
                String tableName = fileName.substring(0, fileName.length() - Global.META_SUFFIX.length());
                if (tableMap.containsKey(tableName)) throw new DuplicateTableException(tableName);

                var columnList = new ArrayList<Column>();
                var reader = new InputStreamReader(new FileInputStream(file));
                var bufferedReader = new BufferedReader(reader);
                String readLine;
                while ((readLine = bufferedReader.readLine()) != null) columnList.add(Column.parseColumn(readLine));
                bufferedReader.close();
                reader.close();
                var table = new Table(databaseName, tableName, columnList);
                System.out.println(table);
                for (Row row : table)
                    System.out.println(row.toString());
                tableMap.put(tableName, table);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }


    // Find position
    public String getDatabasePath() {
        return Global.DBMS_DIR + File.separator + "data" + File.separator + databaseName;
    }

    public String getDatabaseTableFolderPath() {
        return getDatabasePath() + File.separator + "tables";
    }

    public String getDatabaseLogFilePath() {
        return getDatabasePath() + File.separator + "log";
    }

    // Other utils.
    public String getDatabaseName() {
        return this.databaseName;
    }

    public String toString() {
        if (this.tableMap.isEmpty()) return "{\n[DatabaseName: " + databaseName + "]\n" + Global.DATABASE_EMPTY + "}\n";
        StringBuilder result = new StringBuilder("{\n[DatabaseName: " + databaseName + "]\n");
        for (Table table : tableMap.values())
            if (table != null) result.append(table);
        return result + "}\n";
    }
}
