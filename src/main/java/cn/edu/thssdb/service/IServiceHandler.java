package cn.edu.thssdb.service;

import cn.edu.thssdb.parser.SQLHandler;
import cn.edu.thssdb.query.QueryResult;
import cn.edu.thssdb.rpc.thrift.ConnectReq;
import cn.edu.thssdb.rpc.thrift.ConnectResp;
import cn.edu.thssdb.rpc.thrift.DisconnetReq;
import cn.edu.thssdb.rpc.thrift.DisconnetResp;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementReq;
import cn.edu.thssdb.rpc.thrift.ExecuteStatementResp;
import cn.edu.thssdb.rpc.thrift.GetTimeReq;
import cn.edu.thssdb.rpc.thrift.GetTimeResp;
import cn.edu.thssdb.rpc.thrift.IService;
import cn.edu.thssdb.rpc.thrift.Status;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.schema.Row;
import cn.edu.thssdb.type.QueryResultType;
import cn.edu.thssdb.common.Global;
import org.apache.thrift.TException;

import java.util.*;


public class IServiceHandler implements IService.Iface {
    public static Manager manager;
    public long sessionCount = 0;
    public static SQLHandler sqlHandler;

    public IServiceHandler() {
        super();
        manager = Manager.getInstance();
        sqlHandler = new SQLHandler(manager);
    }


    @Override
    public GetTimeResp getTime(GetTimeReq req) {
        GetTimeResp resp = new GetTimeResp();
        resp.setTime(new Date().toString());
        resp.setStatus(new Status(Global.SUCCESS_CODE));
        return resp;
    }

    @Override
    public ConnectResp connect(ConnectReq req) {
        long session = sessionCount++;
        ConnectResp resp = new ConnectResp();
        resp.setStatus(new Status(Global.SUCCESS_CODE));
        resp.setSessionId(session);
        return resp;
    }

    @Override
    public DisconnetResp disconnect(DisconnetReq req) {
        // TODO
        DisconnetResp resp = new DisconnetResp();
        resp.setStatus(new Status(Global.SUCCESS_CODE));
        return resp;
    }

    @Override
    public ExecuteStatementResp executeStatement(ExecuteStatementReq req) {
        ExecuteStatementResp resp = new ExecuteStatementResp();
        long session = req.getSessionId();
        if (session < 0 || session >= sessionCount) {
            Status status = new Status(Global.FAILURE_CODE);
            status.setMsg("please connect first.");
            resp.setStatus(status);
            return resp;
        }

        String command = req.statement;
        String[] statements = command.split(";");
        var results = new ArrayList<QueryResult>();

        for (String statement : statements) {
            statement = statement.trim();
            if (statement.length() == 0) continue;
            QueryResult queryResult;
            if (!manager.inTransactionSessions.contains(session) && !statement.startsWith("begin") && !statement.startsWith("commit")) {
                // transaction for all statement
                // auto commit
                sqlHandler.evaluate("begin transaction", session);
                queryResult = sqlHandler.evaluate(statement, session);
                sqlHandler.evaluate("commit", session);
            } else queryResult = sqlHandler.evaluate(statement, session);
            if (queryResult == null) {
                resp.setStatus(new Status(Global.SUCCESS_CODE));
                resp.setIsAbort(true);
                return resp;
            }
            results.add(queryResult);
        }
        resp.setStatus(new Status(Global.SUCCESS_CODE));
        if (results.isEmpty()) {
            resp.addToColumnsList("empty response");
        } else if (results.get(0) != null && results.get(0).resultType == QueryResultType.SELECT) {
            for (Row row : results.get(0).results) {
                ArrayList<String> the_result = row.toStringList();
                resp.addToRowList(the_result);
            }
            if (!resp.isSetRowList()) {
                resp.rowList = new ArrayList<>();
            }
            for (String column_name : results.get(0).getColumnNames()) {
                resp.addToColumnsList(column_name);
            }
            if (results.size() > 1) {
                resp.addToRowList(List.of("More results are omitted"));
            }
        } else {
            for (QueryResult queryResult : results) {
                if (queryResult == null) resp.addToColumnsList("null");
                else resp.addToColumnsList(queryResult.message);
            }
        }
        return resp;
    }
}
