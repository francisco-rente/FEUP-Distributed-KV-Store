package distributed_system_project;

import distributed_system_project.message.Message;
import distributed_system_project.message.MessageHandler;

import java.io.IOException;
import java.net.DatagramPacket;

import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.net.SocketException;
import java.net.UnknownHostException;

public class StoreUdpServer implements Runnable {

    private final String clusterIp;
    private final int clusterPort;
    private final Store store;
    private MulticastSocket udpServeDatagramSocket = null;
    private InetAddress ip_address;



    public StoreUdpServer(Store store, String clusterIp, Integer clusterPort){
        this.store = store;
        this.clusterIp = clusterIp;
        this.clusterPort = clusterPort;

        try {
            this.ip_address = InetAddress.getByName(this.clusterIp);
            System.out.println("Opening Udp Server");
            this.udpServeDatagramSocket = new MulticastSocket(this.clusterPort);
            this.udpServeDatagramSocket.joinGroup(ip_address);
        } catch (UnknownHostException | SocketException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }


    @Override
    public void run()  {

        try {
            byte[] rbuf = new byte[2000];

            while(true){
                System.out.println("waiting for udp Connections");
                DatagramPacket packet = new DatagramPacket(rbuf, rbuf.length, ip_address, clusterPort);

                if(Thread.currentThread().isInterrupted()){
                    //Only way to close the udp connection
                    break;
                }

                packet = new DatagramPacket(rbuf, rbuf.length);
                udpServeDatagramSocket.receive(packet);
                String messageReceived = new String(packet.getData(), 0, packet.getLength());

                Message received = Message.toObject(messageReceived);

                if(!received.getIp().equals(this.store.getStoreIp()) ){
                    System.out.println("Received an Message");
                    MessageHandler handler = new MessageHandler(this.store, received );
                    handler.run();

                }else{
                    System.out.println("Received the message it sent");
                }

            }

            this.udpServeDatagramSocket.close();

        } catch (UnknownHostException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (SocketException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } 
        catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    public DatagramSocket getDatagramSocket(){
        return this.udpServeDatagramSocket;
    }

    
}
