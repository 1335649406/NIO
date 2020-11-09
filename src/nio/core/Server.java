package nio.core;

import nio.IDealAbnormalDropped;
import nio.IDealNetMessage;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ConcurrentLinkedQueue;

public class Server implements Runnable {
    private ServerSocket server;
    private int port;
    private ConcurrentLinkedQueue<CommunicationUnit>[] queue;
    private IDealNetMessage dealNetMessage;
    private IDealAbnormalDropped dealAbnormalDropped;

    private volatile int curIndex;
    private volatile boolean goon;

    public Server() {
        this.queue = new ConcurrentLinkedQueue[2];
        this.queue[0] = new ConcurrentLinkedQueue();
        this.queue[1] = new ConcurrentLinkedQueue();
        this.curIndex = 0;
        this.goon = false;
    }

    public void startup() {
        if (goon) {
            return;
        }
        try {
            this.server = new ServerSocket(port);
            goon = true;
            new Thread(this).start();
            CommunicationUnitScanner scanner = new CommunicationUnitScanner();
            new Thread(scanner).start();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        goon = false;
        if (server == null || server.isClosed()) {
            return;
        }
        try {
            server.close();
        } catch (IOException e) {
        } finally {
            server = null;
        }
    }

    public void setPort(int port) {
        this.port = port;
    }

    public void setDealNetMessage(IDealNetMessage dealNetMessage) {
        this.dealNetMessage = dealNetMessage;
    }

    @Override
    public void run() {
        while (goon) {
            try {
                Socket client = server.accept();
                CommunicationUnit unit = new CommunicationUnit(client);
                unit.setDealNetMessage(dealNetMessage);
                synchronized (queue) {
                    queue[1 - curIndex].add(unit);
                }
            } catch (IOException e) {
                close();
            }
        }
    }

    public void setDealAbnormalDropped(IDealAbnormalDropped dealAbnormalDropped) {
        this.dealAbnormalDropped = dealAbnormalDropped;
    }

    class CommunicationUnitScanner implements Runnable {

        CommunicationUnitScanner() {
        }

        @Override
        public void run() {
            while (goon) {
                if (queue[curIndex].isEmpty()) {
                    synchronized (queue) {
                        curIndex = 1 - curIndex;
                    }
                    continue;
                }

                CommunicationUnit cu = queue[curIndex].poll();
                if (cu != null && cu.isValue() && !cu.isBusy()) {
                    if (cu.hasMessage()) {
                        new Thread(cu).start();
                    }
                    if (cu.getNumber() == 1000) {
                        dealAbnormalDropped.dealAbnormalDropped(cu);
                    } else {
                        queue[1 - curIndex].add(cu);
                    }
                }
            }
        }
    }
}
