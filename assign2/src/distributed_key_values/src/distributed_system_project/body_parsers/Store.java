package distributed_system_project.body_parsers;

import distributed_system_project.body_parsers.message.Message;
import distributed_system_project.body_parsers.utilities.Pair;
import distributed_system_project.body_parsers.utilities.ShaHasher;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;


public class Store {

    private final String STARTING_MEMBERSHIP_COUNTER = "0";
    private final String DEFAULT_NODE_PORT_STR = "1234";

    private final String folderLocation;

    //distributed_system_project.body_parsers.Store main info
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


    public Store(String storeIp, Integer storePort, String clusterIp, Integer clusterPort) {
        this.storeIp = storeIp;
        this.storePort = storePort;
        this.clusterIp = clusterIp;
        this.clusterPort = clusterPort;
        this.folderLocation = "../node_db/" + storeIp;
        this.membershipLog = this.folderLocation + "membership_log.txt";

        this.udpClusterServer = new StoreUdpServer(this, clusterIp, clusterPort);

        this.cluster = new ArrayList<>();

        System.out.println("Creating TCP server");
        this.tcpConnectionServer = new StoreTcpServer(this, this.storeIp, storePort);

        Thread tcpServer = new Thread(this.tcpConnectionServer);
        tcpServer.start();


        File file = new File(this.folderLocation);
        boolean flag = file.mkdir();

    }

    public static void main(String[] args) {
        if (args.length != 4) {
            System.out.println("Error in number of arguments. Please write something like this on terminal:" +
                    "\n distributed_system_project.body_parsers.Store clusterIp clusterPort storeIp storePort");
            return;
        }

        // read arguments and create distributed_system_project.body_parsers.Store object
        String storeIp = args[2];
        Integer storePort = Integer.parseInt(args[3]);
        String clusterIp = args[0];
        Integer clusterPort = Integer.parseInt(args[1]);
        // create distributed_system_project.body_parsers.Store object

        Store store = new Store(storeIp, storePort, clusterIp, clusterPort);

        while (true) ;

    }


    public String get(String filekey) {
        try {
            // see if file exists in this node, if it does return the file
            if (new File(this.folderLocation + filekey).exists()) {
                // TODO: should this be async?
                byte[] encoded = Files.readAllBytes(Paths.get(this.folderLocation + filekey));
                return new String(encoded, StandardCharsets.UTF_8);
            } else {
                Pair<String, Integer> nearest_node = this.getNearestNode(filekey);

                if (nearest_node.getElement0().isEmpty()) {
                    throw new FileNotFoundException();
                }

                Message request_message = new Message("get", false, nearest_node.getElement0(), nearest_node.getElement1(), filekey);
                this.sendMessage(request_message);
            }
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return "FILE_NOT_FOUND_ERROR";
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return "MESSAGE_SEND_ERROR";
        }
        return "ERROR";
    }


    public String delete(String filekey) {
        try {
            // see if file exists in this node, if it does return the file
            boolean file_exists = new File(this.folderLocation + filekey).exists();
            if (file_exists) {
                // TODO: place tombstone in the file
            }

            List<Pair<String, Integer>> nearest_nodes = this.getNearestNodesWithFile(filekey);

            assert nearest_nodes != null;
            if (nearest_nodes.isEmpty() && !file_exists) {
                throw new FileNotFoundException();
            }

            for (Pair<String, Integer> node : nearest_nodes) {
                Message request_message = new Message("delete", false, node.getElement0(), node.getElement1(), filekey);
                this.sendMessage(request_message);
            }

        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return "FILE_NOT_FOUND_ERROR";
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return "MESSAGE_SEND_ERROR";
        }
        return "ERROR";
    }

    private List<Pair<String, Integer>> getNearestNodesWithFile(String filekey) {
        // TODO: hash the filekey and find the nearest nodes in the cluster list
        //  using 0 - 2 ^ 256 - 1 circular hash and binary search
        return null;
    }

    public String put(String filekey, String value) throws IOException {


        // get the nearest node to this filekey
        Pair<String, Integer> nearest_node = this.getNearestNode(filekey);

        // compare the hash of the filekey to the hash of the nearest node and this one

        int hash_of_filekey = ShaHasher.getHashValue(filekey);
        int hash_of_nearest_node = ShaHasher.getHashValue(nearest_node.getElement0());
        int hash_of_this_node = ShaHasher.getHashValue(this.storeIp);


        // see which has a bigger difference hash_of_nearest_node - hash_of_filekey or hash_of_this_node - hash_of_filekey
        if ((hash_of_nearest_node - hash_of_filekey) % (2 ^ 256 - 1) > (hash_of_this_node - hash_of_filekey) % (2 ^ 256 - 1)) {
            // send the file to the nearest node
            Message request_message = new Message("put", false,
                    nearest_node.getElement0(), nearest_node.getElement1(), filekey + '\n' + value);
            this.sendMessage(request_message);

        } else {
            boolean file_exists = new File(this.folderLocation + filekey).exists();

            if(file_exists) return "ERROR";

            Files.write(Paths.get(this.folderLocation + filekey), value.getBytes(), StandardOpenOption.CREATE);
        }

        // Check if file already exists in this node
        return "SUCCESS";
    }


    private void sendMessage(Message request_message) throws IOException {
        String nodeIp = request_message.getIp();
        int nodePort = request_message.getPort();

        System.out.println("Sending" + request_message.getOperation() + " message to " + nodeIp + ":" + nodePort);
        Socket socket = new Socket(nodeIp, nodePort);

        OutputStream output = socket.getOutputStream();
        PrintWriter writer = new PrintWriter(output, true);
        writer.println(request_message);
    }

    private Pair<String, Integer> getNearestNode(String filekey) {
        String nearest_node_ip = "";
        int nearest_node_port = -1;


        // order the cluster by the first element of array (node ip hash)
        this.cluster.sort(Comparator.comparing(node -> ShaHasher.getHashValue(node.get(0))));

        // TODO: binary search to find the node that is closest to the filekey using 0 - 2 ^ 256 - 1 circular hash
        int index = Collections.binarySearch(this.cluster, new ArrayList<>(Arrays.asList(filekey, "")), Comparator.comparing(node -> ShaHasher.getHashValue(node.get(0))));

        if (index > 0) {
            nearest_node_ip = this.cluster.get(index).get(0);
            nearest_node_port = Integer.parseInt(this.cluster.get(index).get(1));
        }

        return Pair.createPair(nearest_node_ip, nearest_node_port);
    }


    public String getFolderLocation() {
        return folderLocation;
    }

    public String getStoreIp() {
        return storeIp;
    }

    public Integer getStorePort() {
        return storePort;
    }

    public List<ArrayList<String>> getClusterNodes() {
        return cluster;
    }


    public boolean addNodeToCluster(String new_node_ip) {

        if (this.cluster.stream().anyMatch(node -> node.get(0).equals(new_node_ip))) {
            this.cluster.add(new ArrayList<>(Arrays.asList(new_node_ip, DEFAULT_NODE_PORT_STR, STARTING_MEMBERSHIP_COUNTER)));
            return true;
        }

        updateClusterNode(new_node_ip);
        return false;


    }

    public boolean updateClusterNode(String new_node_ip) {
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
