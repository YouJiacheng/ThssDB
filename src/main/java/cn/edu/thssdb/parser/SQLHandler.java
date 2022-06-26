package cn.edu.thssdb.parser;

import cn.edu.thssdb.exception.DatabaseNotExistException;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.common.Global;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CommonTokenStream;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.HashSet;


public class SQLHandler {
    private final Manager manager;

    public SQLHandler(Manager manager) {
        this.manager = manager;
    }

    public QueryResult evaluate(String statement, long session) {
        System.out.println("session:" + session + "  " + statement);
        var currentDB = manager.getCurrentDatabase();
        if (statement.equals(Global.LOG_BEGIN_TRANSACTION)) {
            try {
                if (manager.inTransactionSessions.contains(session))
                    throw new Exception("session already in a transaction.");
                // 禁止恶意begin，非必要不记begin
                if (session >= 0) manager.writeLog(statement, session);
                manager.inTransactionSessions.add(session);
                manager.sessionToLocks.put(session, new HashSet<>());
            } catch (Exception e) {
                return new QueryResult(e.getMessage());
            }
            return new QueryResult("start transaction.");
        }

        if (statement.equals(Global.LOG_COMMIT)) {
            try {
                if (currentDB == null) throw new Exception("current database is unspecified");
                if (!manager.inTransactionSessions.contains(session))
                    throw new Exception("session not in a transaction.");
                // 禁止恶意commit，非必要不记commit
                if (session >= 0) manager.writeLog(statement, session);
                manager.inTransactionSessions.remove(session);
                // rigorous 2PL
                for (var tableName : manager.sessionToLocks.get(session))
                    currentDB.get(tableName).lock.Release(session);
                manager.sessionToLocks.remove(session);
                mayCheckPoint(currentDB.getDatabaseName());
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
        ImpVisitor visitor = new ImpVisitor(manager, session);
        var databaseStmt = LockVisitor.visitDatabaseStmt(stmt);
        if (databaseStmt) // lock database in manager member function.
            return visitor.visitSql_stmt(stmt);

        if (currentDB == null) return new QueryResult("current database is unspecified");
        // never lock manager
        var SLockTables = LockVisitor.visitTableSharedLock(stmt);
        var XLockTables = LockVisitor.visitTableExclusiveLock(stmt);
        var locks = manager.sessionToLocks.get(session);
        try {
            for (var tableName : SLockTables) {
                currentDB.get(tableName).lock.SAcquire(session);
                locks.add(tableName);
            }
            for (var tableName : XLockTables) {
                currentDB.get(tableName).lock.XAcquire(session);
                locks.add(tableName);
            }
        } catch (Exception e) {
            return new QueryResult(e.getMessage());
        }

        // log after parse and lock
        if (session >= 0) // don't log in recover
            if (stmt.insert_stmt() != null || stmt.delete_stmt() != null || stmt.update_stmt() != null)
                manager.writeLog(statement, session); // log only for write table

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

    // NOT Finished
    private void mayCheckPoint(String databaseName) {
        try {
            var path = Path.of(manager.get(databaseName).getDatabaseLogFilePath());
            var fileAttr = Files.readAttributes(path, BasicFileAttributes.class);
            if (fileAttr.isRegularFile() && fileAttr.size() > 50000) {
                System.out.println("Clear database log");
                Files.writeString(path, "", StandardOpenOption.TRUNCATE_EXISTING);
                // TODO: Handle active transaction
                manager.persistDatabase(databaseName);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
