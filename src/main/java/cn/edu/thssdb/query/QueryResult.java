package cn.edu.thssdb.query;


import cn.edu.thssdb.schema.MetaInfo;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.type.QueryResultType;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;


public class QueryResult {

    public final QueryResultType resultType;
    public final String message;

    private List<MetaInfo> metaInfoInfos;
    private List<String> columnNames;

    public List<Row> results;

    public QueryResult(List<Row> rows, List<String> names) {
        resultType = QueryResultType.SELECT;
        message = null;
        results = new ArrayList<>(rows);
        columnNames = names;
    }

    public QueryResult(String msg) {
        resultType = QueryResultType.MESSAGE;
        message = msg;
    }

    public List<String> getColumnNames() {
        return this.columnNames;
    }
}
