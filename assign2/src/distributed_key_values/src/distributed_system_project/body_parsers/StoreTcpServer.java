package distributed_system_project.body_parsers;

import distributed_system_project.body_parsers.message.MessageHandler;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.io.IOException;
import java.net.UnknownHostException;
import java.net.Socket;

public class StoreTcpServer implements Runnable {

    private final String storeIp;
    private final int storePort;
    private final Store store;
    private ServerSocket tcpServerSocket;

    StoreTcpServer(Store store, String storeIp, Integer storePort) {
        this.store = store;
        this.storeIp = storeIp;
        this.storePort = storePort;
    }

    @Override
    public void run() {
        InetAddress ip_address;

        try {
            ip_address = InetAddress.getByName(this.storeIp);
            System.out.println("Server IP: " + ip_address.getHostAddress());
            this.tcpServerSocket = new ServerSocket(this.storePort, 10, ip_address);
            while(true){
                System.out.println("Waiting for connection...");
                Socket socket = this.tcpServerSocket.accept();

                MessageHandler messageHandler = new MessageHandler(this.store, socket);

                messageHandler.run();

                System.out.println("Received distributed_system_project.body_parsers.message.Message");
            }
        
        } catch (UnknownHostException e) {
            // TODO Auto-generated catch blocked
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

  

    }

    public void closeServer(ServerSocket server) {
        try {
            this.tcpServerSocket.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }
}
