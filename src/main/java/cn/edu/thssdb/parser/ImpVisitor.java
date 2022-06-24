package cn.edu.thssdb.parser;


// TODO: add logic for some important cases, refer to given implementations and SQLBaseVisitor.java for structures

import cn.edu.thssdb.common.Global;
import cn.edu.thssdb.exception.DatabaseNotExistException;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.schema.*;
import cn.edu.thssdb.type.ColumnType;
import org.antlr.v4.runtime.RuleContext;

import java.lang.reflect.Array;
import java.util.*;
import java.util.stream.IntStream;

/**
 * When use SQL sentence, e.g., "SELECT avg(A) FROM TableX;"
 * the parser will generate a grammar tree according to the rules defined in SQL.g4.
 * The corresponding terms, e.g., "select_stmt" is a root of the parser tree, given the rules
 * "select_stmt :
 * K_SELECT ( K_DISTINCT | K_ALL )? result_column ( ',' result_column )*
 * K_FROM table_query ( ',' table_query )* ( K_WHERE multiple_condition )? ;"
 * <p>
 * This class "ImpVisit" is used to convert a tree rooted at e.g. "select_stmt"
 * into the collection of tuples inside the database.
 * <p>
 * We give you a few examples to convert the tree, including create/drop/quit.
 * You need to finish the codes for parsing the other rooted trees marked TODO.
 */

public class ImpVisitor extends SQLBaseVisitor<Object> {
    private Manager manager;
    private long session;

    public ImpVisitor(Manager manager, long session) {
        super();
        this.manager = manager;
        this.session = session;
    }

    private Database GetCurrentDB() {
        Database currentDB = manager.getCurrentDatabase();
        if (currentDB == null) {
            throw new DatabaseNotExistException();
        }
        return currentDB;
    }

    public QueryResult visitSql_stmt(SQLParser.Sql_stmtContext ctx) {
        if (ctx.create_db_stmt() != null) return new QueryResult(visitCreate_db_stmt(ctx.create_db_stmt()));
        if (ctx.drop_db_stmt() != null) return new QueryResult(visitDrop_db_stmt(ctx.drop_db_stmt()));
        if (ctx.use_db_stmt() != null) return new QueryResult(visitUse_db_stmt(ctx.use_db_stmt()));
        if (ctx.create_table_stmt() != null) return new QueryResult(visitCreate_table_stmt(ctx.create_table_stmt()));
        if (ctx.drop_table_stmt() != null) return new QueryResult(visitDrop_table_stmt(ctx.drop_table_stmt()));
        if (ctx.insert_stmt() != null) return new QueryResult(visitInsert_stmt(ctx.insert_stmt()));
        if (ctx.delete_stmt() != null) return new QueryResult(visitDelete_stmt(ctx.delete_stmt()));
        if (ctx.update_stmt() != null) return new QueryResult(visitUpdate_stmt(ctx.update_stmt()));
        if (ctx.select_stmt() != null) return visitSelect_stmt(ctx.select_stmt());
        if (ctx.quit_stmt() != null) return new QueryResult(visitQuit_stmt(ctx.quit_stmt()));
        if (ctx.show_meta_stmt() != null) return new QueryResult(visitShow_meta_stmt(ctx.show_meta_stmt()));
        return null;
    }

    public Object visitParse(SQLParser.ParseContext ctx) {
        return visitSql_stmt_list(ctx.sql_stmt_list());
    }

    public Object visitSql_stmt_list(SQLParser.Sql_stmt_listContext ctx) {
        ArrayList<QueryResult> ret = new ArrayList<>();
        for (SQLParser.Sql_stmtContext subCtx : ctx.sql_stmt()) ret.add(visitSql_stmt(subCtx));
        return ret;
    }

    /**
     * 创建数据库
     */
    @Override
    public String visitCreate_db_stmt(SQLParser.Create_db_stmtContext ctx) {
        try {
            manager.createDatabaseIfNotExists(ctx.database_name().getText().toLowerCase());
            manager.persist();
        } catch (Exception e) {
            return e.getMessage();
        }
        return "Create database " + ctx.database_name().getText() + ".";
    }

    /**
     * 删除数据库
     */
    @Override
    public String visitDrop_db_stmt(SQLParser.Drop_db_stmtContext ctx) {
        try {
            manager.deleteDatabase(ctx.database_name().getText().toLowerCase());
        } catch (Exception e) {
            return e.getMessage();
        }
        return "Drop database " + ctx.database_name().getText() + ".";
    }

    /**
     * 切换数据库
     */
    @Override
    public String visitUse_db_stmt(SQLParser.Use_db_stmtContext ctx) {
        try {
            manager.switchDatabase(ctx.database_name().getText().toLowerCase());
        } catch (Exception e) {
            return e.getMessage();
        }
        return "Switch to database " + ctx.database_name().getText() + ".";
    }

    /**
     * 删除表格
     */
    @Override
    public String visitDrop_table_stmt(SQLParser.Drop_table_stmtContext ctx) {
        try {
            GetCurrentDB().drop(ctx.table_name().getText().toLowerCase());
        } catch (Exception e) {
            return e.getMessage();
        }
        return "Drop table " + ctx.table_name().getText() + ".";
    }

    /**
     * 创建表格
     */
    private ColumnType getColumnType(SQLParser.Type_nameContext ctx) {
        if (ctx.T_INT() != null) return ColumnType.INT;
        if (ctx.T_LONG() != null) return ColumnType.LONG;
        if (ctx.T_FLOAT() != null) return ColumnType.FLOAT;
        if (ctx.T_DOUBLE() != null) return ColumnType.DOUBLE;
        if (ctx.T_STRING() != null) return ColumnType.STRING;
        return null;
    }

    private int getMaxLength(SQLParser.Type_nameContext ctx) {
        var maxLen = ctx.NUMERIC_LITERAL();
        if (maxLen != null) return Integer.parseInt(maxLen.getText());
        return 0;
    }

    private boolean getNotNull(SQLParser.Column_defContext ctx) {
        var constraint = ctx.column_constraint(0);
        return (constraint != null) && (constraint.K_NULL() != null);
    }

    private List<String> getPrimaryKeys(SQLParser.Create_table_stmtContext ctx) {
        var constraint = ctx.table_constraint();
        if (constraint != null) {
            return constraint.column_name().stream().map(RuleContext::getText).toList();
        }
        return Collections.emptyList();
    }

    @Override
    public String visitCreate_table_stmt(SQLParser.Create_table_stmtContext ctx) {
        try {
            var primaryKeys = getPrimaryKeys(ctx);
            var columns = new ArrayList<Column>();
            for (var col : ctx.column_def()) {
                var name = col.column_name().getText();
                var type = getColumnType(col.type_name());
                var maxLen = getMaxLength(col.type_name());
                if (type == ColumnType.STRING && maxLen <= 0) {
                    throw new Exception("String column must have positive max length");
                }
                var primary = primaryKeys.contains(name);
                var notNull = getNotNull(col) || primary; // primary key is not null!
                columns.add(new Column(name, type, primary, notNull, maxLen));
            }
            GetCurrentDB().create(ctx.table_name().getText(), columns.toArray(new Column[0]));
            return "Create table " + ctx.table_name().getText() + ".";
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    /**
     * TODO
     * 显示表格
     */
    public String visitShow_meta_stmt(SQLParser.Show_meta_stmtContext ctx) {
        try {
            var table = GetCurrentDB().get(ctx.table_name().getText());
            var s = new StringBuilder();
            s.append(table.tableName);
            s.append("(\n");
            for (var c : table.columns) {
                s.append(c.representation());
                s.append("\n");
            }
            s.append(")");
            return s.toString();
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    /**
     * TODO
     * 表格项插入
     */
    @Override
    public String visitInsert_stmt(SQLParser.Insert_stmtContext ctx) {
        try {
            var table = GetCurrentDB().get(ctx.table_name().getText());
            var table_columns = table.columns;
            var table_columns_name = table_columns.stream().map(Column::getColumnName).toList();
            var columns_name = ctx.column_name().stream().map(RuleContext::getText).toList();
            if (columns_name.isEmpty()) {
                columns_name = table_columns_name;
            }
            var columns_idx = new ArrayList<Integer>();
            for (var name : columns_name) {
                var idx = table_columns_name.indexOf(name);
                if (idx == -1) {
                    throw new Exception("column " + name + " doesn't exist in table definition");
                }
                columns_idx.add(idx);
            }
            var columns = columns_idx.stream().map(table_columns::get).toList();
            var num_table_columns = table_columns.size();
            var num_columns = columns_name.size();
            for (var e : ctx.value_entry()) {
                var entry_map = new TreeMap<Integer, Cell>();
                var literal_values = e.literal_value();
                if (literal_values.size() != num_columns) {
                    throw new Exception("arity of value is not consistent with column");
                }
                System.out.println(literal_values.stream().map(RuleContext::getText).toList());
                for (int i = 0; i < num_columns; ++i) {
                    var v = literal_values.get(i);
                    String s;
                    if (v.K_NULL() != null) {
                        s = Global.ENTRY_NULL; // parseEntry only recognize "null"
                    } else {
                        s = v.getText();
                    }
                    entry_map.put(columns_idx.get(i), Column.parseEntry(s, columns.get(i)));
                }
                // complete unspecified columns
                var entry_completed = new ArrayList<Cell>();
                for (int i = 0; i < num_table_columns; ++i) {
                    var c = table_columns.get(i);
                    if (entry_map.containsKey(i)) {
                        entry_completed.add(entry_map.get(i));
                    } else {
                        entry_completed.add(Column.parseEntry(Global.ENTRY_NULL, c));
                    }
                }
                table.insert(new Row(entry_completed));
            }
            return "INSERT succeed";
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    private boolean evaluateMultipleCondition(SQLParser.Multiple_conditionContext ctx, Map<String, Row> tableToRow, Map<String, List<Column>> tableToColumns, String tableName) throws Exception {
        if (ctx.condition() != null) return evaluateCondition(ctx.condition(), tableToRow, tableToColumns, tableName);
        var lhs = evaluateMultipleCondition(ctx.multiple_condition(0), tableToRow, tableToColumns, tableName);
        var rhs = evaluateMultipleCondition(ctx.multiple_condition(1), tableToRow, tableToColumns, tableName);
        if (ctx.AND() != null) return lhs && rhs;
        if (ctx.OR() != null) return lhs || rhs;
        throw new Exception();
    }

    private boolean evaluateCondition(SQLParser.ConditionContext ctx, Map<String, Row> tableToRow, Map<String, List<Column>> tableToColumns, String tableName) throws Exception {
        var lhs = evaluateExpression(ctx.expression(0), tableToRow, tableToColumns, tableName);
        var rhs = evaluateExpression(ctx.expression(1), tableToRow, tableToColumns, tableName);
        var cp = ctx.comparator();
        assert cp != null;
        if (cp.EQ() != null) return lhs.SQLCompareTo(rhs, List.of(0));
        if (cp.NE() != null) return lhs.SQLCompareTo(rhs, List.of(-1, 1));
        if (cp.LE() != null) return lhs.SQLCompareTo(rhs, List.of(-1, 0));
        if (cp.GE() != null) return lhs.SQLCompareTo(rhs, List.of(0, 1));
        if (cp.LT() != null) return lhs.SQLCompareTo(rhs, List.of(-1));
        if (cp.GT() != null) return lhs.SQLCompareTo(rhs, List.of(1));
        throw new Exception();
    }

    private Cell evaluateExpression(SQLParser.ExpressionContext ctx, Map<String, Row> tableToRow, Map<String, List<Column>> tableToColumns, String tableName) throws Exception {
        if (ctx.comparer() != null) {
            return evaluateComparer(ctx.comparer(), tableToRow, tableToColumns, tableName);
        }
        var subExpr = ctx.expression();
        var arg0 = evaluateExpression(subExpr.get(0), tableToRow, tableToColumns, tableName);
        var arg1 = subExpr.size() > 1 ? evaluateExpression(subExpr.get(1), tableToRow, tableToColumns, tableName) : null;
        if (ctx.ADD() != null) return arg0.arithmetic(arg1, Double::sum);
        if (ctx.SUB() != null) return arg0.arithmetic(arg1, (a, b) -> a - b);
        if (ctx.MUL() != null) return arg0.arithmetic(arg1, (a, b) -> a * b);
        if (ctx.DIV() != null) return arg0.arithmetic(arg1, (a, b) -> a / b);
        return arg0; // (expression) case
    }

    private Cell evaluateComparer(SQLParser.ComparerContext ctx, Map<String, Row> tableToRow, Map<String, List<Column>> tableToColumns, String tableName) throws Exception {
        var columnFullName = ctx.column_full_name();
        if (columnFullName != null) {
            if (columnFullName.table_name() != null) tableName = columnFullName.table_name().getText();
            var columns = tableToColumns.get(tableName);
            var columnName = columnFullName.column_name().getText();
            var idx = columns.stream().map(Column::getColumnName).toList().indexOf(columnName);
            if (idx == -1) throw new Exception("column " + columnName + " doesn't exist in table definition");
            return tableToRow.get(tableName).getEntries().get(idx);
        }
        var v = ctx.literal_value();
        assert v != null;
        if (v.NUMERIC_LITERAL() != null) {
            return new Cell(Double.valueOf(v.getText()));
        }
        if (v.STRING_LITERAL() != null) {
            var s = v.getText();
            return new Cell(s.substring(1, s.length() - 1));
        }
        assert v.K_NULL() != null;
        return new Cell(null);
    }

    private List<Row> filterTable(SQLParser.Multiple_conditionContext ctx, Table table) throws Exception {
        var index = table.index;
        var name = table.tableName;
        var data = new ArrayList<Row>();
        for (var pair : index) {
            var row = pair.right;
            if (ctx == null || evaluateMultipleCondition(ctx, Map.of(name, row), Map.of(name, table.columns), name)) {
                data.add(row);
            }
        }
        return data;
    }

    /**
     * TODO
     * 表格项删除
     */
    @Override
    public String visitDelete_stmt(SQLParser.Delete_stmtContext ctx) {
        try {
            var table = GetCurrentDB().get(ctx.table_name().getText());
            var filteredTable = filterTable(ctx.multiple_condition(), table);
            for (var row : filteredTable)
                table.delete(row);
            return "DELETE " + filteredTable.size() + " row(s)";
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    /**
     * TODO
     * 表格项更新
     */
    @Override
    public String visitUpdate_stmt(SQLParser.Update_stmtContext ctx) {
        try {
            var table = GetCurrentDB().get(ctx.table_name().getText());
            var name = table.tableName;
            var columns = table.columns;
            var setColumnName = ctx.column_name().getText();
            var setIdx = columns.stream().map(Column::getColumnName).toList().indexOf(setColumnName);
            if (setIdx < 0)
                throw new Exception("column " + setColumnName + " doesn't exist in table definition");
            var primaryIdx = table.primaryIndex;
            var filteredTable = filterTable(ctx.multiple_condition(), table);
            for (var row : filteredTable) {
                var entries = new ArrayList<>(row.getEntries());
                var primaryKey = entries.get(primaryIdx);
                var val = evaluateExpression(ctx.expression(), Map.of(name, row), Map.of(name, columns), name);
                entries.set(setIdx, val);
                table.update(primaryKey, new Row(entries));
            }
            return "UPDATE " + filteredTable.size() + " row(s)";
        } catch (Exception e) {
            return e.getMessage();
        }
    }

    /**
     * TODO
     * 表格项查询
     */
    @Override
    public QueryResult visitSelect_stmt(SQLParser.Select_stmtContext ctx) {
        return null;
    }

    /**
     * 退出
     */
    @Override
    public String visitQuit_stmt(SQLParser.Quit_stmtContext ctx) {
        try {
            manager.quit();
        } catch (Exception e) {
            return e.getMessage();
        }
        return "Quit.";
    }
}
