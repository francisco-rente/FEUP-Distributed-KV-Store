import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.util.*;


public class Node {
    
    private static String starting_membership_counter = "0";

    private final String folderLocation; 

    private final String id;
    private final Integer node_port; 

    private final List<ArrayList<String>> clusterNodes; 
    private final String membershipLog; 
    private final String cluster_ip;
    private final Integer cluster_port;

    private final ServerSocket serverSocket; 


    public Node(String id, Integer node_port, String cluster_ip, Integer cluster_port) throws IOException {
        this.id = id;
        this.node_port = node_port;
        this.cluster_ip = cluster_ip;
        this.cluster_port = cluster_port;
        this.folderLocation = "../node_db/" + id; 
        this.membershipLog = this.folderLocation + "membership_log.txt"; 

        this.clusterNodes = new ArrayList<ArrayList<String>>(); 
        this.serverSocket = null; 

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
        return id;
    }

    public Integer getNode_port() {
        return node_port;
    }

    public List<ArrayList<String>> getClusterNodes() {
        return clusterNodes;
    }


    public boolean addNodeToCluster( String new_node_ip){
        
        if(this.clusterNodes.stream().anyMatch(node -> node.get(0).equals(new_node_ip))) {
            this.clusterNodes.add(new ArrayList<String>(Arrays.asList(new_node_ip, starting_membership_counter)));
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
        return cluster_ip;
    }

    public Integer getCluster_port() {
        return cluster_port;
    }


}
