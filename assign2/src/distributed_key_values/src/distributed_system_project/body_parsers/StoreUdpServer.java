package distributed_system_project.body_parsers;

import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;

public class StoreUdpServer implements Runnable {

    private final String clusterIp;
    private final int clusterPort;
    private final Store store;

    StoreUdpServer(Store store ,String clusterIp, Integer clusterPort){
        this.store = store;
        this.clusterIp = clusterIp;
        this.clusterPort = clusterPort;
      
    }


    @Override
    public void run()  {

        InetAddress ip_address;

        try {
            ip_address = InetAddress.getByName(this.clusterIp);
            DatagramSocket udpServeDatagramSocket = new DatagramSocket(this.clusterPort, ip_address);


        } catch (UnknownHostException e) {
            // TODO Auto-generated catch blocked
            e.printStackTrace();
        } catch (SocketException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } 

        
    }

    
}
