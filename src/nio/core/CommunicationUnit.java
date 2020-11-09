package nio.core;

import nio.IDealNetMessage;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class CommunicationUnit implements Runnable {
    private Socket client;
    private DataInputStream dis;
    private DataOutputStream dos;
    private IDealNetMessage dealNetMessage;

    private boolean isBusy;
    private boolean isValue;
    private int number;

    private int port;
    private String ip;

    CommunicationUnit(Socket client) {
        try {
            this.number = 0;
            this.client = client;
            this.port = client.getPort();
            this.ip = client.getInetAddress().getHostAddress();
            this.dis = new DataInputStream(client.getInputStream());
            this.dos = new DataOutputStream(client.getOutputStream());
            this.isBusy = false;
            this.isValue = true;
        } catch (IOException e) {
            this.isValue = false;
            close();
        }
    }

    public void sendMessage(String message) {
        try {
            this.dos.writeUTF(message);
        } catch (IOException e) {
            this.isValue = false;
            close();
        }
    }

    boolean hasMessage() {
        int available = 0;
        try {
            available = dis.available();
        } catch (IOException e) {
            return false;
        }
        boolean res = available != 0;
        if (res) {
            number = 0;
        } else {
            number++;
        }
        return res;
    }

    public void close() {
        if (client != null && !client.isClosed()) {
            try {
                client.close();
            } catch (IOException e) {
            } finally {
                client = null;
            }
        }

        if (dis != null) {
            try {
                dis.close();
            } catch (IOException e) {
            } finally {
                dis = null;
            }
        }

        if (dos != null) {
            try {
                dos.close();
            } catch (IOException e) {
            } finally {
                dos = null;
            }
        }
    }

    @Override
    public void run() {
        if (dealNetMessage == null) {
            return;
        }
        try {
            this.isBusy = true;
            String s = dis.readUTF();
            dealNetMessage.dealNetMessage(s);
            this.isBusy = false;
        } catch (IOException e) {
            this.isValue = false;
            close();
        }
    }

    void setDealNetMessage(IDealNetMessage dealNetMessage) {
        this.dealNetMessage = dealNetMessage;
    }

    boolean isBusy() {
        return isBusy;
    }

    boolean isValue() {
        return isValue;
    }

    public int getPort() {
        return port;
    }

    public String getIp() {
        return ip;
    }

    public int getNumber() {
        return number;
    }
}
