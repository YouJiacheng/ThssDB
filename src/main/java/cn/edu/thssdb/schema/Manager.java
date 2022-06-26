package cn.edu.thssdb.schema;

import cn.edu.thssdb.exception.DatabaseNotExistException;
import cn.edu.thssdb.exception.FileIOException;
import cn.edu.thssdb.parser.SQLHandler;
import cn.edu.thssdb.common.Global;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class Manager {
    private final HashMap<String, Database> databases;
    public Database currentDatabase;
    public ArrayList<Long> inTransactionSessions;
    public static SQLHandler sqlHandler;
    public final Map<Long, Set<String>> sessionToLocks;

    public static Manager getInstance() {
        return Manager.ManagerHolder.INSTANCE;
    }

    public Manager() {
        databases = new HashMap<>();
        currentDatabase = null;
        inTransactionSessions = new ArrayList<>();
        sqlHandler = new SQLHandler(this);
        sessionToLocks = new HashMap<>();
        File managerFolder = new File(Global.DBMS_DIR + File.separator + "data");
        if (!(managerFolder.exists() || managerFolder.mkdirs())) throw new RuntimeException("create file failed");
        recover();
        createDatabaseIfNotExists("db");
        persistMeta();
    }

    public void deleteDatabase(String databaseName, long session) {
        var db = get(databaseName); // synchronized
        try {
            db.lock.XAcquire(session); // Deadlock if acquire in synchronized
            db.dropDatabase(); // synchronized by db
            synchronized (this) {
                databases.remove(databaseName);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            db.lock.XRelease(session);
        }
    }

    public void switchDatabase(String databaseName, long session) {
        try {
            var db = get(databaseName); // synchronized
            db.lock.SAcquire(session); // Deadlock if acquire in synchronized
            synchronized (this) {
                if (currentDatabase != db) // release if db change
                    if (currentDatabase != null) currentDatabase.lock.SRelease(session);
                currentDatabase = db;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class ManagerHolder {
        private static final Manager INSTANCE = new Manager();

        private ManagerHolder() {

        }
    }

    public Database getCurrentDatabase() {
        return currentDatabase;
    }

    // utils:
    // NO Lock Needed
    public synchronized void quit() {
        for (var db : databases.values()) {
            db.persistMeta();
            db.persistTable();
        }
        persistMeta();
    }

    // NO Lock Needed
    public synchronized Database get(String databaseName) {
        if (!databases.containsKey(databaseName)) throw new DatabaseNotExistException(databaseName);
        return databases.get(databaseName);
    }

    // NO Lock Needed
    public synchronized void createDatabaseIfNotExists(String databaseName) {
        if (!databases.containsKey(databaseName)) databases.put(databaseName, new Database(databaseName));
        currentDatabase = get(databaseName);
    }

    public synchronized void persistMeta() {
        try {
            FileOutputStream fos = new FileOutputStream(Manager.getManagerDataFilePath());
            OutputStreamWriter writer = new OutputStreamWriter(fos);
            for (String databaseName : databases.keySet())
                writer.write(databaseName + "\n");
            writer.close();
            fos.close();
        } catch (Exception e) {
            throw new FileIOException(Manager.getManagerDataFilePath());
        }
    }

    // NO Lock Needed
    public synchronized void persistDatabase(String databaseName) {
        var db = get(databaseName);
        db.persistMeta();
        db.persistTable();
        persistMeta();
    }


    // Log control and recover from logs.
    public void writeLog(String statement, long sId) {
        String logFilename = currentDatabase.getDatabaseLogFilePath();
        try {
            FileWriter writer = new FileWriter(logFilename, true);
            writer.write(sId + "\n" + statement + "\n");
            writer.close();
        } catch (Exception e) {
            throw new FileIOException(logFilename);
        }
    }

    static class LogItem {
        public long session;
        public String statement;
        public boolean committed;

        LogItem(long s, String st) {
            this.session = s;
            this.statement = st;
            this.committed = false;
        }
    }

    // TODO: read Log in transaction to recover.
    public void readLog(String databaseName) throws IOException {
        System.out.println("??!! try to recover database " + databaseName + " from log");
        var logLines = Files.readAllLines(Path.of(getDatabaseLogFilePath(databaseName)));
        var logItems = new ArrayList<LogItem>();
        for (var it = logLines.iterator(); it.hasNext(); ) {
            var session = Long.parseLong(it.next());
            logItems.add(new LogItem(session, it.next()));
        }

        // process, get committed state of every log
        var reversedLogItems = new ArrayList<>(logItems); // shallow copy
        Collections.reverse(reversedLogItems);
        var committedSessionSet = new HashSet<Long>();
        for (var i : reversedLogItems) {
            if (i.statement.equals(Global.LOG_COMMIT)) committedSessionSet.add(i.session);
            if (committedSessionSet.contains(i.session)) i.committed = true;
            if (i.statement.equals(Global.LOG_BEGIN_TRANSACTION)) committedSessionSet.remove(i.session);
        }

        // recover the committed logs
        for (var i : logItems) {
            if (i.committed) {
                System.out.println("??!! session: " + i.session + " statement: " + i.statement);
                sqlHandler.evaluate(i.statement, -i.session - 2);
            } else {
                System.out.println("??!! UNCOMMITTED ITEM session: " + i.session + " statement: " + i.statement);
            }
        }

    }

    public synchronized void recover() {
        try {
            System.out.println("??!! try to recover manager");
            var databases = Files.readAllLines(Path.of(getManagerDataFilePath()));
            try {
                for (var database : databases) {
                    System.out.println("??!!" + database);
                    createDatabaseIfNotExists(database);
                    readLog(database);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (Exception ignored) {
        }
    }

    // Get positions
    public static String getManagerDataFilePath() {
        return Global.DBMS_DIR + File.separator + "data" + File.separator + "manager";
    }

    // get database log file path, same as Database.getDatabaseLogFilePath(String)
    public static String getDatabaseLogFilePath(String databaseName) {
        return Global.DBMS_DIR + File.separator + "data" + File.separator + databaseName + File.separator + "log";
    }
}
