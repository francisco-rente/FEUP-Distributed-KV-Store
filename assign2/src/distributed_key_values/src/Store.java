import java.io.File;
import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.util.*;
import java.net.InetAddress;




public class Store {

    private final String STARTING_MEMBERSHIP_COUNTER = "0";
    
    private final String folderLocation; 

    //Store main info
    private final String storeIp;
    private final Integer storePort;

    private final List<ArrayList<String>> cluster; 
    private final String membershipLog; 

    //UDP cluster transport variables
    private StoreUdpServer udpClusterServer;
    private final String clusterIp;
    private final Integer clusterPort;

    //TCP membership variables
    private StoreTcpServer tcpConnectionServer;
    


    public Store(String id, Integer storePort, String clusterIp, Integer clusterPort) throws IOException {
        this.storeIp = id;
        this.storePort = storePort;
        this.clusterIp = clusterIp;
        this.clusterPort = clusterPort;
        this.folderLocation = "../node_db/" + id; 
        this.membershipLog = this.folderLocation + "membership_log.txt"; 

        this.udpClusterServer = new StoreUdpServer(this, clusterIp, clusterPort);

        this.cluster = new ArrayList<ArrayList<String>>(); 

        this.tcpConnectionServer = new StoreTcpServer(this, this.storeIp, storePort); 



        try {
            File file = new File(this.folderLocation);
            boolean flag = file.mkdir();  
        } catch (Exception e) {
            //TODO: handle exception
        }
          

    }   


    public String getFolderLocation() {
        return folderLocation;
    }

    public String getId() {
        return storeIp;
    }

    public Integer getStore_port() {
        return storePort;
    }

    public List<ArrayList<String>> getClusterNodes() {
        return cluster;
    }


    public boolean addNodeToCluster(String new_node_ip){
        
        if(this.cluster.stream().anyMatch(node -> node.get(0).equals(new_node_ip))) {
            this.cluster.add(new ArrayList<String>(Arrays.asList(new_node_ip, STARTING_MEMBERSHIP_COUNTER)));
            return true;
        }

        updateClusterNode(new_node_ip);
        return false;
           

    }

    public boolean updateClusterNode(String new_node_ip){
        return true;
        
    }

    public String getMembershipLog() {
        return membershipLog;
    }

    public String getCluster_ip() {
        return clusterIp;
    }

    public Integer getCluster_port() {
        return clusterPort;
    }


}
