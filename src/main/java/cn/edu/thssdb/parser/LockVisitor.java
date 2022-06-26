package cn.edu.thssdb.parser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class LockVisitor {

    static public boolean visitDatabaseStmt(SQLParser.Sql_stmtContext ctx) {
        return (ctx.create_db_stmt() != null || ctx.drop_db_stmt() != null || ctx.use_db_stmt() != null);
    }

    static public List<String> visitTableSharedLock(SQLParser.Sql_stmtContext ctx) {
        if (ctx.show_meta_stmt() != null) return List.of(ctx.show_meta_stmt().table_name().getText());
        var data = new ArrayList<String>();
        var select = ctx.select_stmt();
        if (select != null) for (var q : select.table_query())
            for (var t : q.table_name())
                data.add(t.getText());
        return data;
    }

    static public List<String> visitTableExclusiveLock(SQLParser.Sql_stmtContext ctx) {
        if (ctx.drop_table_stmt() != null) return List.of(ctx.drop_table_stmt().table_name().getText());
        // if (ctx.create_table_stmt() != null) return List.of(ctx.create_table_stmt().table_name().getText());
        // can't lock table for create table since table haven't been created
        if (ctx.insert_stmt() != null) return List.of(ctx.insert_stmt().table_name().getText());
        if (ctx.delete_stmt() != null) return List.of(ctx.delete_stmt().table_name().getText());
        if (ctx.update_stmt() != null) return List.of(ctx.update_stmt().table_name().getText());
        return Collections.emptyList();
    }
}
