package cn.edu.thssdb.parser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TableVisitor {

    static public List<String> visitLockS(SQLParser.ParseContext ctx) {
        var data = new ArrayList<String>();
        for (var stmt : ctx.sql_stmt_list().sql_stmt())
            data.addAll(visitLockSSingleStmt(stmt));
        return data;
    }

    static public List<String> visitLockX(SQLParser.ParseContext ctx) {
        var data = new ArrayList<String>();
        for (var stmt : ctx.sql_stmt_list().sql_stmt())
            data.addAll(visitLockXSingleStmt(stmt));
        return data;
    }

    static public List<String> visitLockSSingleStmt(SQLParser.Sql_stmtContext ctx) {
        var data = new ArrayList<String>();
        var select = ctx.select_stmt();
        if (select != null)
            for (var q : select.table_query())
                for (var t : q.table_name())
                    data.add(t.getText());
        return data;
    }

    static public List<String> visitLockXSingleStmt(SQLParser.Sql_stmtContext ctx) {
        // create table only need Lock-X on database, but drop table need Lock-X on table
        if (ctx.drop_table_stmt() != null) return List.of(ctx.drop_table_stmt().table_name().getText());
        if (ctx.insert_stmt() != null) return List.of(ctx.insert_stmt().table_name().getText());
        if (ctx.delete_stmt() != null) return List.of(ctx.delete_stmt().table_name().getText());
        if (ctx.update_stmt() != null) return List.of(ctx.update_stmt().table_name().getText());
        return Collections.emptyList();
    }
}
