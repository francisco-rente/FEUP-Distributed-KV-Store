package distributed_system_project;

import distributed_system_project.message.MessageHandler;

import java.net.*;
import java.io.IOException;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class StoreTcpServer implements Runnable {

    private static final long THREAD_TIMEOUT = 1000;
    private static final int THREADS_IN_POOL = 8;

    private final String storeIp;
    private final int storePort;
    private final Store store;
    private ServerSocket tcpServerSocket;
    // AsynchronousServerSocketChannel server;

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
            ExecutorService executor = java.util.concurrent.Executors.newFixedThreadPool(THREADS_IN_POOL);


            while (!executor.isShutdown()) {
                //TODO: how to close the executor service with a timeout?
                // executor.shutdown();

                Socket socket = this.tcpServerSocket.accept();
                
                System.out.println("Accepted connection from: " + socket.getInetAddress().getHostAddress() + ":" + socket.getPort() + "\n");

                MessageHandler messageHandler = new MessageHandler(this.store, socket);

                System.out.println("RUNNING THREAD\n");

                // messageHandler.run();
                executor.submit(messageHandler);
            }
            executor.awaitTermination(THREAD_TIMEOUT, TimeUnit.MILLISECONDS);
        } catch (IOException | InterruptedException e) {
            // TODO Auto-generated catch blocked
            e.printStackTrace();
        } // TODO Auto-generated catch block

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
