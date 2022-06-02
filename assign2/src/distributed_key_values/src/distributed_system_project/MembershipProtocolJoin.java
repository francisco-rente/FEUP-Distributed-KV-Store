package distributed_system_project;

import distributed_system_project.Store;
import distributed_system_project.message.Message;
import distributed_system_project.message.MessageHandler;
import distributed_system_project.utilities.SocketsIo;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

public class MembershipProtocolJoin implements Runnable {

    private final Store store;

    public MembershipProtocolJoin(Store store){
        this.store = store;
    }

    @Override
    public void run() {
    
        InetAddress ip_adressCluster;
        InetAddress ip_adressTcp;
        try {
            ip_adressCluster = InetAddress.getByName(this.store.getCluster_ip());
            ip_adressTcp = InetAddress.getByName(this.store.getStoreIp());
            
            DatagramSocket udpSocket = new DatagramSocket();
            //Create message to send
            Message newMessage = new Message("join", false, this.store.getStoreIp(), Store.getMembershipPort(), "");
            SocketsIo.sendUdpMessage(newMessage, udpSocket, ip_adressCluster, this.store.getCluster_port());
            
            //Create TcpServer on storeIp and MembershipPort
            ServerSocket tcpServerSocket = new ServerSocket(Store.getMembershipPort(), 10, ip_adressTcp );
            tcpServerSocket.setSoTimeout(Store.getTimeoutTime());

            List<String> store_ips = new ArrayList<String>(); 
            int timeoutCounter = 0;
            int validResponses = 0;

            while(true){
                try{
                    System.out.println("Waiting for connections...");
                    Socket socket = tcpServerSocket.accept();

                    String messageString = SocketsIo.readFromSocket(socket);

                    Message message =  Message.toObject(messageString);
                    
                    if(!store_ips.contains(message.getIp())){
                        store_ips.add(message.getIp());

                        MessageHandler messageHandler = new MessageHandler(this.store, message);
                        messageHandler.run();
                        validResponses++;
                        if(validResponses ==3){
                            this.store.startUdpServer();
                            this.store.startSendingPeriodicMembership();
                            break;
                        }
                        
                    }
                    
                }
                catch(SocketTimeoutException e){
                    timeoutCounter++;
                    System.out.println("Number of timeouts: "+ timeoutCounter);
                    if(timeoutCounter == 3){
                        if(validResponses == 0){
                            store.initializeMembership();
                        }
                        else{
                            System.out.println("Entered membership without consensus");
                            this.store.startUdpServer();
                            this.store.startSendingPeriodicMembership();
                        }
                        break;
                    }

                    SocketsIo.sendUdpMessage(newMessage, udpSocket, ip_adressCluster, this.store.getCluster_port());
                    
                }
                
            }

            udpSocket.close();
            tcpServerSocket.close();


        } catch (UnknownHostException | SocketException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    
}
