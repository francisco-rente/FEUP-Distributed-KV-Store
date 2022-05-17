import java.net.InetAddress;
import java.net.ServerSocket;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.UnknownHostException;


public class StoreTcpServer implements Runnable {

    private final String storeIp;
    private final int storePort;
    private final Store store;
    private ServerSocket tcpServeDatagramSocket;

    StoreTcpServer(Store store ,String storeIp, Integer storePort){
        this.store = store;
        this.storeIp = storeIp;
        this.storePort = storePort;        
    }




    @Override
    public void run()  {
        InetAddress ip_address;

        try {
            ip_address = InetAddress.getByName(this.storeIp);
            this.tcpServeDatagramSocket = new ServerSocket(this.storePort, 10, ip_address);

        } catch (UnknownHostException e) {
            // TODO Auto-generated catch blocke
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } 

        
    }

    public void closeServer(ServerSocket server) throws IOException   {
        try{
            tcpServeDatagramSocket.close();                 
        }
        catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } 

    } 
}
