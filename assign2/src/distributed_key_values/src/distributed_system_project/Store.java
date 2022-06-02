package distributed_system_project;

import distributed_system_project.message.Message;
import distributed_system_project.message.MessageCodes;
import distributed_system_project.message.MessageType;
import distributed_system_project.message.body_parsers.DeleteMessageBodyParser;
import distributed_system_project.message.body_parsers.GetMessageBodyParser;
import distributed_system_project.message.body_parsers.PutMessageBodyParser;
import distributed_system_project.utilities.Pair;
import distributed_system_project.utilities.ShaHasher;
import distributed_system_project.utilities.SocketsIo;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.stream.Collectors;


public class Store {

    private static final String STARTING_MEMBERSHIP_COUNTER = "0";
    private static final int MEMBERSHIP_PORT = 7777;
    private static final int TIMEOUT_TIME = 10000;

    private final String folderLocation;

    //Store main info
    private final String storeIp;
    private final Integer storePort;
    private String storeId;

    private final List<ArrayList<String>> cluster;
    private PriorityQueue<String> last32Logs;
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
        this.last32Logs = new PriorityQueue<>();


        this.storeId = ShaHasher.getHashString(storeIp);

        //Creates Store Id

        this.folderLocation = "./node_db/" + storeId;
        this.membershipLog = this.folderLocation + "/membership_log.txt";

        this.cluster = new ArrayList<>();
        this.cluster.add(new ArrayList<>(Arrays.asList(storeIp, STARTING_MEMBERSHIP_COUNTER)));

        if (storeIp.equals("127.0.0.1"))
            this.cluster.add(new ArrayList<>(Arrays.asList("127.0.0.2", STARTING_MEMBERSHIP_COUNTER)));
        else if (storeIp.equals("127.0.0.2"))
            this.cluster.add(new ArrayList<>(Arrays.asList("127.0.0.1", STARTING_MEMBERSHIP_COUNTER)));


        System.out.println("Creating TCP server");
        this.tcpConnectionServer = new StoreTcpServer(this, this.storeIp, storePort);


        Thread tcpServer = new Thread(this.tcpConnectionServer);
        tcpServer.start();

        //Creates Store dir and membershiplog.txt
        File directory = new File(this.folderLocation);
        directory.mkdirs();

        File membershipLog = new File(this.membershipLog);
        try {
            membershipLog.createNewFile();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }


    public static void main(String[] args) {
        if (args.length != 4) {
            System.out.println("Error in number of arguments. Please write something like this on temrinal:\n Store clusterIp clusterPort storeIp storePort");
            return;
        }

        // read arguments and create Store object
        String storeIp = args[2];
        Integer storePort = Integer.parseInt(args[3]);
        String clusterIp = args[0];
        Integer clusterPort = Integer.parseInt(args[1]);
        // create Store object

        Store store = new Store(storeIp, storePort, clusterIp, clusterPort);

        while (true) {
        }
    }

    /*
             _   _  _    _  _  _  _    _
        | | | || |_ (_)| |(_)| |_ (_) ___  ___
        | |_| ||  _|| || || ||  _|| |/ -_)(_-/
         \___/  \__||_||_||_| \__||_|\___|/__/

     */

    public PriorityQueue<String> getLast32Logs() {
        return last32Logs;
    }

    public void setLast32Logs(PriorityQueue<String> last32Logs) {
        this.last32Logs = last32Logs;
    }

    public void addLog(String log) {
        if (this.last32Logs.size() == 32) {
            last32Logs.poll();
        }

        last32Logs.add(log);
    }


    public static int getTimeoutTime() {
        return TIMEOUT_TIME;
    }

    public static String getStartingMembershipCounter() {
        return STARTING_MEMBERSHIP_COUNTER;
    }

    public StoreUdpServer getUdpServer() {
        return this.udpClusterServer;
    }

    public static int getMembershipPort() {
        return MEMBERSHIP_PORT;
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

    /*
    public boolean addNodeToCluster(String new_node_ip){

        if(this.cluster.stream().anyMatch(node -> node.get(0).equals(new_node_ip))) {
            this.cluster.add(new ArrayList<String>(Arrays.asList(new_node_ip, STARTING_MEMBERSHIP_COUNTER)));
            return true;
        }

        updateClusterNode(new_node_ip);
        return false;


    }
    */

    public void addStoreToCluster(String storeIp, String membershipCounter) {

        for (ArrayList<String> list : this.cluster) {
            if (list.get(0).equals(storeIp)) {
                return;
            }
        }

        ArrayList<String> store = new ArrayList<String>();
        store.add(storeIp);
        store.add(membershipCounter);


        this.cluster.add(store);
        String log = storeIp + " " + membershipCounter;
        addLog(log);

        FileSystem.writeOnFile(this.membershipLog, log);
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



    /*
         ___  _                              _ __                  _    _
        / __|| |_  ___  _ _  ___        ___ | '_ \ ___  _ _  __ _ | |_ (_) ___  _ _   ___
        \__ \|  _|/ _ \| '_|/ -_)      / _ \| .__// -_)| '_|/ _` ||  _|| |/ _ \| ' \ (_-/
        |___/ \__|\___/|_|  \___|      \___/|_|   \___||_|  \__/_| \__||_|\___/|_||_|/__/

     */

    public String get(String filekey) {

        System.out.println("ASKING FOR FILE: " + filekey);

        // Search for the file in this store
        String file_content;
        file_content = searchDirectory(filekey);
        if (file_content.equals(MessageCodes.FILE_NOT_FOUND)) return file_content;


        // Search for the file in the cluster
        Pair<String, Integer> nearest_node = this.getNearestNodeForKey(filekey);

        Message request_message = new Message(MessageType.GET.toString(), false,
                this.storeIp, this.storePort, filekey);


        String response_string = this.sendGetRequest(request_message,
                new Pair<>(nearest_node.getElement0(), this.storePort));
        if (!response_string.equals(MessageCodes.ERROR_CONNECTING) && !response_string.equals("NOT_FOUND"))
            return response_string;

        // Try its replicas if it's not found
        List<Pair<String, Integer>> replicas = this.getStoreReplicas(nearest_node.getElement0());

        for (Pair<String, Integer> replica : replicas) {
            if (replica.getElement0().equals(this.storeIp)) continue; // cannot loop the request to myself
            response_string = this.sendGetRequest(request_message, replica);
            if (response_string != null && !response_string.equals("NOT_FOUND")) return response_string;
        }

        return MessageCodes.GET_FAIL;
    }


    public String delete(String filekey, boolean isTestClient) {
        Pair<String, Integer> nearest_node = this.getNearestNodeForKey(filekey);
        List<Pair<String, Integer>> replicas = this.getStoreReplicas(nearest_node.getElement0());

        boolean success = true;

        // If I'm a replica of the store that owns the file or the owner, delete it
        if (nearest_node.getElement0().equals(this.storeIp) ||
                replicas.contains(new Pair<>(this.storeIp, this.storePort))) {
            if (searchDirectory(filekey).equals(MessageCodes.FILE_NOT_FOUND))
                return MessageCodes.FILE_NOT_FOUND;
            deleteFile(filekey);
            // If the request is not from a test client, we don't need to alert the other replicas
            if (!isTestClient) return MessageCodes.FILE_DELETED;
        }

        replicas.add(nearest_node); // send the request to the owner and its replicas
        for (Pair<String, Integer> replica : replicas) {
            if (replica.getElement0().equals(this.storeIp)) continue; // cannot loop the request to myself
            String response_string = this.sendDeleteRequest(new Message("delete", false,
                    this.storeIp, this.storePort, filekey), replica);
            if (response_string.equals(MessageCodes.FILE_NOT_FOUND)) success = false;
        }

        return success ? MessageCodes.DELETE_SUCCESS : MessageCodes.ERROR_CONNECTING;

    }


    public String put(String filekey, String value, boolean isTestClient) {

        Pair<String, Integer> nearest_node = this.getNearestNodeForKey(filekey);
        List<Pair<String, Integer>> replicas = this.getStoreReplicas(nearest_node.getElement0());

        boolean success = true;

        // If I'm a replica of the store that owns the file or the owner, delete it
        if (nearest_node.getElement0().equals(this.storeIp) ||
                replicas.contains(new Pair<>(this.storeIp, this.storePort))) {
            //TODO: Doubt, should I continue even if I can't save it myself if I'm the owner?
            // Should I signal for a Rollback?
            // TRY AGAIN IF FAILURE?
            if (saveFile(filekey, value).equals(MessageCodes.ERROR_SAVING_FILE)) success = false;
            if (!isTestClient) return MessageCodes.PUT_SUCCESS;
        }

        replicas.add(nearest_node); // send the request to the owner and its replicas
        for (Pair<String, Integer> replica : replicas) {
            if (replica.getElement0().equals(this.storeIp)) continue; // cannot loop the request to myself
            String response_string = this.sendPutRequest(new Message("put", false,
                    storeIp, storePort, filekey + '\n' + value), replica);
            if (response_string.equals(MessageCodes.PUT_FAIL)) success = false;
        }

        return success ? MessageCodes.PUT_SUCCESS : MessageCodes.ERROR_CONNECTING;

    }


    /*
             ___       __ _                 _
        | _ \ ___ / _` | _  _  ___  ___| |_  ___
        |   // -_)\__. || || |/ -_)(_-/|  _|(_-/
        |_|_\\___|   |_| \_._|\___|/__/ \__|/__/

     */


    String sendGetRequest(Message request_message, Pair<String, Integer> node) {
        Message response_message = sendMessageAndWaitResponse(request_message, node);
        if (request_message == null) return MessageCodes.ERROR_CONNECTING;

        String response_body = new GetMessageBodyParser(response_message.getBody()).parse();
        if (!response_body.equals(MessageCodes.GET_FAIL)) return response_body;
        return MessageCodes.FILE_NOT_FOUND;
    }

    private String sendPutRequest(Message request_message, Pair<String, Integer> store) {
        Message response_message = sendMessageAndWaitResponse(request_message, store);
        if (request_message == null) return MessageCodes.ERROR_CONNECTING;

        String response_body = new PutMessageBodyParser(response_message.getBody()).parseResponseToRequest();
        if (response_body.equals(MessageCodes.PUT_SUCCESS)) return response_body;
        return MessageCodes.PUT_FAIL;
    }

    String sendDeleteRequest(Message request_message, Pair<String, Integer> node) {
        Message response_message = sendMessageAndWaitResponse(request_message, node);
        if (request_message == null) return MessageCodes.ERROR_CONNECTING;

        String response_body = new DeleteMessageBodyParser(response_message.getBody()).parseResponseToRequest();
        if (response_body.equals(MessageCodes.DELETE_SUCCESS)) return response_body;
        return MessageCodes.FILE_NOT_FOUND;
    }


    Message sendMessageAndWaitResponse(Message message, Pair<String, Integer> node) {
        Socket socket;
        socket = this.sendMessage(message, node);
        if (socket == null) return null;
        return this.getMessage(socket);
    }

    private Socket sendMessage(Message request_message, Pair<String, Integer> storeInfo) {
        String nodeIp = storeInfo.getElement0();
        int nodePort = this.storePort;

        System.out.println("Sending " + request_message.getOperation() + " message to " + nodeIp + ":" + nodePort);
        try {
            Socket socket = new Socket(nodeIp, nodePort); //TODO: Should this be a new thread?
            socket.setSoTimeout(5000);
            SocketsIo.sendStringToSocket(request_message.toString(), socket);
            return socket;
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }


    private Message getMessage(Socket socket) {
        try {
            socket.setSoTimeout(5000);
            String messageString = SocketsIo.readFromSocket(socket);
            System.out.println("getMessage Received message: " + messageString);
            assert messageString != null;
            return Message.toObject(messageString);
        } catch (SocketException e) {
            e.printStackTrace();
            return null;
        }
    }

    boolean sendFilesToNode(ArrayList<String> files) {
        for (String file : files) {
            try {
                this.put(file, Files.readAllLines(Paths.get(this.folderLocation + "/" + file)).get(0), false);
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            }
        }
        return true;
    }




    /*
             ___  _  _                 _                    __ _
        | __|(_)| | ___        ___| |_  ___  _ _  __ _ / _` | ___
        | _| | || |/ -_)      (_-/|  _|/ _ \| '_|/ _` |\__. |/ -_)
        |_|  |_||_|\___|      /__/ \__|\___/|_|  \__/_||___/ \___|

     */


    String searchDirectory(String filekey) {
        if (!new File(this.folderLocation + "/" + filekey).exists()) return MessageCodes.FILE_NOT_FOUND;
        byte[] encoded = new byte[0];
        try {
            encoded = Files.readAllBytes(Paths.get(this.folderLocation + "/" + filekey));
        } catch (IOException e) {
            e.printStackTrace();
            return MessageCodes.ERROR_READING_FILE;
        }
        //TODO: Does something have to happen for tombstones?
        return new String(encoded, StandardCharsets.UTF_8);
    }

    boolean deleteFile(String filekey) {
        File file = new File(this.folderLocation + "/" + filekey);
        //TODO: what does renameTo return if error
        return file.renameTo(new File(this.folderLocation + "/" + filekey + ".deleted"));
    }


    private String saveFile(String filekey, String value) {
        try {
            boolean file_exists = new File(this.folderLocation + "/" + filekey).exists();
            if (file_exists) return MessageCodes.FILE_EXISTS;
            Files.write(Paths.get(this.folderLocation + "/" + filekey), value.getBytes(), StandardOpenOption.CREATE);
            return MessageCodes.PUT_SUCCESS;
        } catch (IOException e) {
            e.printStackTrace();
            return MessageCodes.ERROR_SAVING_FILE;
        }
    }


    ArrayList<String> getFiles() {
        ArrayList<String> files = new ArrayList<>();
        File folder = new File(this.folderLocation);
        for (File file : Objects.requireNonNull(folder.listFiles())) {
            if (file.getName().endsWith(".deleted")) continue;
            files.add(file.getName());
        }
        return files;
    }


    /*
         _  _            _            ___  _           _
        | || | __ _  ___| |_         / __|(_) _ _  __ | | ___
        | __ |/ _` |(_-/|   \       | (__ | || '_|/ _|| |/ -_)
        |_||_|\__/_|/__/|_||_|       \___||_||_|  \__||_|\___|

     */


    List<Pair<String, Integer>> getStoreReplicas(String storeIP) {
        String ip_hash = ShaHasher.getHashString(storeIP);
        ArrayList<Pair<String, Integer>> replicas = new ArrayList<>();

        // only one store in cluster (myself)
        if (this.cluster.size() == 1) return replicas;

        // TODO: sort abuse
        List<ArrayList<String>> sortedCluster = sortCluster();
        int index = this.getNodePosition(sortedCluster, ip_hash);


        for (int i = 1; i >= 0; i--) {
            // if (i < 0) i = sortedCluster.size() - 1;
            int replica_index = (index + i) % sortedCluster.size();
            ArrayList<String> replica_node = sortedCluster.get(replica_index);
            replicas.add(new Pair<>(sortedCluster.get(i).get(0), this.storePort));
        }

        return replicas.stream().distinct().collect(Collectors.toList());
    }


    public List<ArrayList<String>> sortCluster() {
        List<ArrayList<String>> availableNodes =
                new ArrayList<>(this.cluster.stream().filter(node -> Integer.parseInt(node.get(1)) % 2 == 0).collect(Collectors.toList()));
        availableNodes.sort(Comparator.comparing((ArrayList<String> node) -> ShaHasher.getHashString(node.get(0))));
        return availableNodes;
    }

    private int getNodePosition(List<ArrayList<String>> sorted_cluster, String ip_hash) {
        // get nodes with active counter
        return this.getIndexOfNearestNode(sorted_cluster, ip_hash);
    }


    ArrayList<String> filesToMove(String new_node_ip) {
        ArrayList<String> files_to_be_moved = new ArrayList<>();
        String hash_of_new_node = ShaHasher.getHashString(new_node_ip);

        ArrayList<String> files = this.getFiles();
        Collections.sort(files);

        int index = Collections.binarySearch(files, hash_of_new_node);
        if (index < 0) index = -index - 1;

        files_to_be_moved.add(files.get(index));
        return files_to_be_moved;
    }


    private Pair<String, Integer> getNearestNodeForKey(String filekey) {
        String nearest_node_ip;
        int nearest_node_port;

        // get nodes with active counter
        List<ArrayList<String>> availableNodes =
                new ArrayList<>(this.cluster.stream().filter(node -> Integer.parseInt(node.get(1)) % 2 == 0).collect(Collectors.toList()));

        System.out.println("Available nodes: " + availableNodes);

        // sort available nodes ip value using a lambda that uses compareTo
        availableNodes.sort(Comparator.comparing((ArrayList<String> node) -> ShaHasher.getHashString(node.get(0))));

        int index = this.getIndexOfNearestNode(availableNodes, filekey);

        System.out.println("Store Index for the operation: " + index + " node : " + availableNodes.get(index));

        nearest_node_ip = availableNodes.get(index).get(0);
        nearest_node_port = this.storePort;
        return Pair.createPair(nearest_node_ip, nearest_node_port);
    }


    Integer getIndexOfNearestNode(List<ArrayList<String>> availableNodes, String filekey) {

        List<String> node_ips = availableNodes.stream().map(node -> node.get(0)).collect(Collectors.toList());
        List<String> hash_values = node_ips.stream().map(ShaHasher::getHashString).collect(Collectors.toList());

        // print hashed values of nodes ip
        System.out.println("Hashed values of nodes ip: " + hash_values);
        int index = Collections.binarySearch(hash_values, filekey);

        System.out.println("Binary search index: " + index);

        if (index < 0) {
            index = -index - 1; // revert the negative index
            if (index == availableNodes.size()) index = 0; // if it overflows, set it to the first node
        }
        return index;
    }


    private ArrayList<String> getPredecessors(String nodeIp) {
        return getStoreReplicas(nodeIp).stream().
                map(Pair::getElement0).collect(Collectors.toCollection(ArrayList::new));
    }

    private List<String> getSucessors(String storeIp) {

        ArrayList<String> successors = new ArrayList<>();

        if (this.cluster.size() == 1) return successors;


        String nodeHash = ShaHasher.getHashString(storeIp);
        ArrayList<Pair<String, Integer>> replicas = new ArrayList<>();

        List<ArrayList<String>> sortedCluster = sortCluster();
        int index = this.getNodePosition(sortedCluster, nodeHash);


        for (int i = 1; i <= 2; i++) {
            int replicaIndex = (index + i) % sortedCluster.size();
            replicas.add(Pair.createPair(sortedCluster.get(replicaIndex).get(0), this.storePort));
        }

        return replicas.stream().map(Pair::getElement0).distinct().collect(Collectors.toList());
    }

    private List<String> getPreferenceList(String filekey) {

        List<String> preference_list = new ArrayList<>();

        Pair<String, Integer> nearestNodeForKey = this.getNearestNodeForKey(filekey);
        String nearestNodeIp = nearestNodeForKey.getElement0();

        preference_list.add(nearestNodeIp);
        preference_list.addAll(this.getPredecessors(nearestNodeIp));
        return preference_list;
    }

    public String membershipJoinHandler(String newStoreIp) {
        // Join node to the cluster - DONE
        String nodeHash = ShaHasher.getHashString(newStoreIp);

        // Get 2 predecessors
        List<String> predecessorsIps = this.getPredecessors(newStoreIp);
        // Get 2 successors
        List<String> sucessorsIps = this.getSucessors(newStoreIp);

        if (!predecessorsIps.contains(this.storeIp) && !sucessorsIps.contains(this.storeIp))
            return MessageCodes.UPDATED_WITH_JOIN; // If I'm not in it exit

        ArrayList<String> files = getFiles();

        for (String filekey : files) {
            List<String> preferenceListIps = this.getPreferenceList(filekey); // get the list of stores in charge

            String value = this.searchDirectory(filekey);
            Message message = new Message(MessageType.PUT.toString(), false,
                    newStoreIp, this.storePort, filekey + "\n" + value);
            if (!preferenceListIps.contains(this.storeIp)) { // No longer in charge of the file
                deleteFile(filekey); // delete the file from the directory
                if (sucessorsIps.get(0).equals(this.storeIp)) // if I'm the first successor send the file to new node
                    sendPutRequest(message, Pair.createPair(sucessorsIps.get(0), this.storePort));
                continue;
            }
            // I'm still responsible, but new is too, send it to new
            sendPutRequest(message, Pair.createPair(sucessorsIps.get(1), this.storePort));
        }

        return MessageCodes.UPDATED_WITH_JOIN;
    }


    public void join() {

        this.udpClusterServer = new StoreUdpServer(this, clusterIp, clusterPort);
        Thread udpServer = new Thread(this.udpClusterServer);
        udpServer.start();

        MembershipProtocolJoin server = new MembershipProtocolJoin(this);
        Thread thread = new Thread(server);

        thread.start();

    }

    public void initializeMembership() {
        System.out.println("This is the first Membership Store");

        ArrayList<String> storeInfo = new ArrayList<String>();
        storeInfo.add(storeIp);
        storeInfo.add(Store.STARTING_MEMBERSHIP_COUNTER);

        this.cluster.add(storeInfo);

        String log = storeIp + " " + Store.STARTING_MEMBERSHIP_COUNTER;
        addLog(log);
        FileSystem.writeOnFile(membershipLog, log);
    }


}
