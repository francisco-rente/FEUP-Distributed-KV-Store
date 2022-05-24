package distributed_system_project.body_parsers;

import distributed_system_project.body_parsers.message.MessageHandler;
import distributed_system_project.body_parsers.message.MessageHandlerForAsyncSocket;

import java.net.InetAddress;
import java.net.ServerSocket;
import java.io.IOException;
import java.net.UnknownHostException;
import java.net.Socket;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public class StoreTcpServer implements Runnable {

    private static final long THREAD_TIMEOUT = 100;
    private static int THREADS_IN_POOL = 8;

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

            // new thread pool
            ExecutorService executor = java.util.concurrent.Executors.newFixedThreadPool(THREADS_IN_POOL);
            AsynchronousChannelGroup group = AsynchronousChannelGroup.withThreadPool(executor);
            AsynchronousServerSocketChannel server = AsynchronousServerSocketChannel.open(group);

            server.bind(null);
            server.accept(null, new CompletionHandler<AsynchronousSocketChannel, Store>() {
                @Override
                public void completed(AsynchronousSocketChannel channel, Store attachment) {

                    // accept next connection
                    server.accept(null, this);

                    // handle connection
                    MessageHandlerForAsyncSocket messageHandler = new MessageHandlerForAsyncSocket(attachment, channel);
                    messageHandler.run();
                }

                @Override
                public void failed(Throwable exc, Store attachment) {
                    System.out.println("Failed to accept connection");
                }
            });

            while (!group.isTerminated()) {
                group.awaitTermination(THREAD_TIMEOUT, TimeUnit.SECONDS);
            }


        } catch (InterruptedException | IOException e) {
            // TODO Auto-generated catch blocked
            e.printStackTrace();
        }


    }

    /*public void closeServer(ServerSocket server) {
        try {
            this.tcpServerSocket.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }*/
}
