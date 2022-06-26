package cn.edu.thssdb.parser;

import cn.edu.thssdb.exception.DatabaseNotExistException;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.schema.Database;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.schema.Table;
import cn.edu.thssdb.common.Global;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class SQLHandler {
    private final Manager manager;
    private final static String INSERT = "insert";
    private final static String DELETE = "delete";
    private final static String UPDATE = "update";
    private final static String BEGIN = "begin";
    private final static String COMMIT = "commit";
    private final static String SELECT = "select";
    private static final String[] CMD_SET_WITHOUT_SCB = {INSERT, DELETE, UPDATE};

    public SQLHandler(Manager manager) {
        this.manager = manager;
    }

    public QueryResult evaluate(String statement, long session) {
        String stmt_head = statement.split("\\s+")[0];

        System.out.println("session:" + session + "  " + statement);
        Database currentDB = manager.getCurrentDatabase();
        if (currentDB == null) throw new DatabaseNotExistException();
        String databaseName = currentDB.getDatabaseName();
        if (statement.equals(Global.LOG_BEGIN_TRANSACTION)) {
            System.out.println("BEGIN");
            try {
                if (manager.currentSessions.contains(session)) throw new Exception("session already in a transaction.");
                // 禁止恶意begin，非必要不记begin
                if (session >= 0) manager.writeLog(statement, session);
                manager.currentSessions.add(session);
                manager.x_lockDict.put(session, new ArrayList<>());
                manager.s_lockDict.put(session, new ArrayList<>());
            } catch (Exception e) {
                return new QueryResult(e.getMessage());
            }
            return new QueryResult("start transaction.");
        }

        if (statement.equals(Global.LOG_COMMIT)) {
            try {
                if (!manager.currentSessions.contains(session)) throw new Exception("session not in a transaction.");
                System.out.println("COMMIT");
                // 禁止恶意commit，非必要不记commit
                if (session >= 0) manager.writeLog(statement, session);
                manager.currentSessions.remove(session);
                ArrayList<String> table_list = manager.x_lockDict.get(session);
                for (String table_name : table_list) {
                    Table currentTable = currentDB.get(table_name);
                    currentTable.releaseXLock(session);
                }
                table_list = manager.s_lockDict.get(session);
                for (String table_name : table_list) {
                    Table currentTable = currentDB.get(table_name);
                    currentTable.releaseSLock(session);
                }
                table_list.clear();

                String databaseLogFilename = Database.getDatabaseLogFilePath(databaseName);
                File file = new File(databaseLogFilename);
                BasicFileAttributes basicFileAttributes = Files.readAttributes(file.toPath(), BasicFileAttributes.class);
                if (file.exists() && basicFileAttributes.isRegularFile() && basicFileAttributes.size() > 50000) {
                    System.out.println("Clear database log");
                    try {
                        FileWriter writer = new FileWriter(databaseLogFilename);
                        writer.write("");
                        writer.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                    manager.persistDatabase(databaseName);
                }

            } catch (Exception e) {
                return new QueryResult(e.getMessage());
            }
            return new QueryResult("commit transaction.");
        }
        SQLParser.Sql_stmtContext stmt;
        try {
            stmt = parseStatement(statement);
        } catch (Exception e) {
            String message = "Exception: illegal SQL statement! Error message: " + e.getMessage();
            return new QueryResult(message);
        }

        // log after parse
        if (session >= 0 && Arrays.asList(CMD_SET_WITHOUT_SCB).contains(stmt_head.toLowerCase()))
            manager.writeLog(statement, session);

        ImpVisitor visitor = new ImpVisitor(manager);
        var lockXManager = LockVisitor.visitManagerExclusiveLock(stmt);
        var lockSDB = LockVisitor.visitDatabaseSharedLock(stmt);
        var lockXDB = LockVisitor.visitDatabaseExclusiveLock(stmt);
        var lockSTables = LockVisitor.visitTableSharedLock(stmt);
        var lockXTables = LockVisitor.visitTableExclusiveLock(stmt);
        ArrayList<String> x_lock_list = manager.x_lockDict.get(session);
        ArrayList<String> s_lock_list = manager.s_lockDict.get(session);
        ArrayList<String> new_x_list = new ArrayList<>(x_lock_list);
        ArrayList<String> new_s_list = new ArrayList<>(s_lock_list);
        System.out.println("For session " + session);
        System.out.println("====BEFORE====");
        for (String s : x_lock_list){
            System.out.println("Write locked " + s);
        }
        for (String s : s_lock_list){
            System.out.println("Read locked " + s);
        }

        System.out.println("Pid is:" + ManagementFactory.getRuntimeMXBean().getName().split("@")[0]);

        System.out.println("====AFTER====");
        for (String s : lockXTables){
            if(!x_lock_list.contains(s)) {
                if(s_lock_list.contains(s)){
                    currentDB.get(s).releaseSLock(session);
                    new_s_list.remove(s);
                }
                currentDB.get(s).takeXLock(session);
                currentDB.get(s).printLock();
                System.out.println("Write locked " +s);
                new_x_list.add(s);
            }
        }
        for (String s : lockSTables){
            if((!s_lock_list.contains(s)) && (!new_x_list.contains(s))) {
                currentDB.get(s).takeSLock(session);
                currentDB.get(s).printLock();
                new_s_list.add(s);
                System.out.println("Read locked " +s);
            }
        }

        manager.s_lockDict.put(session, new_s_list);
        manager.x_lockDict.put(session, new_x_list);
        return visitor.visitSql_stmt(stmt);
    }

    public SQLParser.Sql_stmtContext parseStatement(String statement) {
        SQLLexer lexer = new SQLLexer(CharStreams.fromString(statement));
        lexer.removeErrorListeners();
        lexer.addErrorListener(SQLErrorListener.instance);
        CommonTokenStream tokenStream = new CommonTokenStream(lexer);

        SQLParser parser = new SQLParser(tokenStream);
        parser.removeErrorListeners();
        parser.addErrorListener(SQLErrorListener.instance);
        return parser.sql_stmt();
    }

}
