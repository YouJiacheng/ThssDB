package cn.edu.thssdb.server;

import cn.edu.thssdb.rpc.thrift.IService;
import cn.edu.thssdb.schema.Manager;
import cn.edu.thssdb.service.IServiceHandler;
import cn.edu.thssdb.common.Global;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThssDB {

    private static final Logger logger = LoggerFactory.getLogger(ThssDB.class);

    public static ThssDB getInstance() {
        return ThssDBHolder.INSTANCE;
    }

    public static void main(String[] args) {
        ThssDB server = ThssDB.getInstance();
        server.start();
    }

    private void start() {
        IServiceHandler handler = new IServiceHandler();
        var processor = new IService.Processor<>(handler);
        Runnable setup = () -> setUp(processor);
        new Thread(setup).start();
    }

    private static void setUp(IService.Processor<IServiceHandler> processor) {
        try {
            TServerSocket transport = new TServerSocket(Global.DEFAULT_SERVER_PORT);
            var server = new TThreadPoolServer(new TThreadPoolServer.Args(transport).processor(processor));
            logger.info("Starting ThssDB ...");
            server.serve();
        } catch (TTransportException e) {
            logger.error(e.getMessage());
        }
    }

    private static class ThssDBHolder {
        private static final ThssDB INSTANCE = new ThssDB();

        private ThssDBHolder() {

        }
    }
}
