package distributed_system_project;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


import distributed_system_project.message.Message;
import distributed_system_project.message.MessageCodes;
import distributed_system_project.message.MessageType;
import distributed_system_project.message.body_parsers.DeleteMessageBodyParser;
import distributed_system_project.message.body_parsers.GetMessageBodyParser;
import distributed_system_project.message.body_parsers.PutMessageBodyParser;
import distributed_system_project.utilities.Pair;
import distributed_system_project.utilities.ShaHasher;
import distributed_system_project.utilities.SocketsIo;


public class Store {

    private static final String STARTING_MEMBERSHIP_COUNTER = "0";
    private static final int MEMBERSHIP_PORT = 7777;
    private static final int TIMEOUT_TIME = 5000;

    private final String folderLocation;

    //Store main info
    private final String storeIp;
    private final Integer storePort;
    private String storeId;

    public static int UPDATE_CLUSTER_JOIN = 0;
    public static int UPDATE_CLUSTER_LEAVE = 1;

    public ScheduledThreadPoolExecutor periodicMembershipSender = null;

    private List<ArrayList<String>> cluster;
    private List<String> last32Logs;
    private final String membershipLog;

    //UDP cluster transport variables
    private Thread udpClusterServer;
    private final String clusterIp;
    private final Integer clusterPort;

    //TCP membership variables
    private StoreTcpServer tcpConnectionServer;


    public Store(String storeIp, Integer storePort, String clusterIp, Integer clusterPort) {
        this.storeIp = storeIp;
        this.storePort = storePort;
        this.clusterIp = clusterIp;
        this.clusterPort = clusterPort;
        this.last32Logs = new ArrayList<>();

        this.storeId = ShaHasher.getHashString(this.storeIp);

        //Creates Store Id
        this.folderLocation = "./node_db/" + storeId;
        this.membershipLog = this.folderLocation + "/membership_log.txt";

        this.cluster = new ArrayList<>();

        System.out.println("Creating TCP server");
        this.tcpConnectionServer = new StoreTcpServer(this, this.storeIp, storePort);

        Thread tcpServer = new Thread(this.tcpConnectionServer);
        tcpServer.start();

        //Creates Store dir and membershiplog.txt
        File directory = new File(this.folderLocation);
        directory.mkdirs();

        this.readClusterInformationFromLog();

        this.createClusterFromLogs();

        //In case of crash and if this store was in cluster
        //It will start with udp server on and
        if (this.getMembershipCounter() % 2 == 0) {
            this.startUdpServer();
        }

    }


    public List<String> getLast32Logs() {
        return last32Logs;
    }

    /**
     * Reads from the memebersip log file the membership information and stores it on the field last32Logs
     * If such file is not found, it will create a new one
     */
    private void readClusterInformationFromLog() {
        File file = new File(this.membershipLog);

        try {
            if (!file.createNewFile()) {
                FileInputStream fis = new FileInputStream(file);
                BufferedReader br = new BufferedReader(new InputStreamReader(fis));
                for (String tmp; (tmp = br.readLine()) != null; ) {
                    addLog(tmp);
                }


            }
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }


    }

    /**
     * Creates the cluster array from the last32Logs
     */
    private void createClusterFromLogs() {

        this.cluster.clear();
        for (String log : this.last32Logs) {
            String[] node = log.split(" ");
            ArrayList<String> nodeInfo = new ArrayList<String>();
            nodeInfo.add(node[0]);
            nodeInfo.add(node[1]);

            this.cluster.add(nodeInfo);

        }
    }

    /**
     * Overwrites the membership log with the current cluster information
     */
    private void writeLogs() {
        FileSystem.writeTextOnFile(this.membershipLog, this.last32Logs);
    }

    /**
     * Adds a new membership log to the last32Logs
     *
     * @param last32Logs
     */
    public void setLast32Logs(List<String> last32Logs) {

        for (String log : this.last32Logs) {
            this.addLog(log);
        }
    }

    /**
     * Adds a log to the last32Logs
     * In case a log of such store ip already exists, it is replaced
     * This function changes the membership log and cluster
     *
     * @param log
     */
    public void addLog(String log) {

        boolean changedLog = false;
        String[] logInfo = log.split(" ");
        for (int i = 0; i < this.last32Logs.size(); i++) {
            String[] tmpInfo = this.last32Logs.get(i).split(" ");

            //If exists already on list, ends here
            if (log.equals(this.last32Logs.get(i))) {
                return;
            }

            //If already has info on list but membership is different
            if (logInfo[0].equals(tmpInfo[0]) && !logInfo[1].equals(tmpInfo[1])) {
                String newLog = logInfo[0] + " " + Math.max(Integer.parseInt(tmpInfo[1]), Integer.parseInt(logInfo[1]));
                this.last32Logs.set(i, newLog);
                changedLog = true;

                break;
            }

        }

        //If not there, adds to the log
        if (!changedLog) last32Logs.add(log);
        this.createClusterFromLogs();
        this.writeLogs();
    }


    public int getMembershipCounter() {
        int count = -1;

        for (String log : this.last32Logs) {
            List<String> tmpLog = FileSystem.logToInfo(log);
            if (tmpLog.get(0).equals(this.storeIp)) {
                count = Integer.parseInt(tmpLog.get(1));
            }
        }
        return count;
    }


    public static int getTimeoutTime() {
        return TIMEOUT_TIME;
    }

    public static String getStartingMembershipCounter() {
        return STARTING_MEMBERSHIP_COUNTER;
    }

    public Thread getUdpServer() {
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

    public void setClusterNodes(List<ArrayList<String>> cluster) {
        this.cluster = cluster;
    }

    /**
     * Updates the mmebership information of the store which ip is storeIp
     *
     * @param storeIp     The ip address of the store
     * @param joinOrLeave If the store is entering or leaving the cluster. If variable is 0 it is joining, if it is 1 it is leaving
     */
    public void updateStoreToCluster(String storeIp, int joinOrLeave) {

        for (int i = 0; i < last32Logs.size(); i++) {
            ArrayList<String> tmpLog = FileSystem.logToInfo(last32Logs.get(i));

            if (tmpLog.get(0).equals(storeIp)) {
                if (Integer.parseInt(tmpLog.get(1)) % 2 != joinOrLeave) {
                    String newLog = tmpLog.get(0) + " " + String.valueOf(Integer.parseInt(tmpLog.get(1)) + 1);
                    addLog(newLog);
                }

                return;
            }
        }

        //Case of joining the cluster of the first time
        if (joinOrLeave == 0) {
            addLog(storeIp + " " + Store.STARTING_MEMBERSHIP_COUNTER);
        }

        return;

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

    public String get(String filekey, boolean isTestClient) {

        System.out.println("ASKING FOR FILE: " + filekey);

        // Search for the file in this store
        String file_content;
        file_content = searchDirectory(filekey);
        if (!file_content.equals(MessageCodes.FILE_NOT_FOUND) && !file_content.equals(MessageCodes.ERROR_CONNECTING))
            return file_content;

        // If I'm not the first to be contacted, another node as already asked me and the owner/other replicas
        if (!isTestClient) return MessageCodes.FILE_NOT_FOUND;

        // Search for the file in the cluster
        Pair<String, Integer> nearest_node = this.getNearestNodeForKey(filekey);

        Message request_message = new Message(MessageType.GET.toString(), false,
                this.storeIp, this.storePort, filekey);
        String response_string;

        if (!nearest_node.getElement0().equals(this.storeIp)) { // Prevent loop back
            response_string = this.sendGetRequest(request_message,
                    new Pair<>(nearest_node.getElement0(), this.storePort));
            if (wasGetSuccessful(response_string)) return response_string;
        }

        // Try its replicas if it's not found
        List<Pair<String, Integer>> replicas = this.getStoreReplicas(nearest_node.getElement0());
        System.out.println("REPLICAS: " + replicas.stream().map(Pair::getElement0).collect(Collectors.toList()));
        for (Pair<String, Integer> replica : replicas) {
            if (replica.getElement0().equals(this.storeIp)) continue; // cannot loop the request to myself
            response_string = this.sendGetRequest(request_message, replica);
            if (wasGetSuccessful(response_string)) return response_string;
        }

        return MessageCodes.GET_FAIL;
    }


    public String delete(String filekey, boolean isTestClient) {
        Pair<String, Integer> nearest_node = this.getNearestNodeForKey(filekey);
        List<Pair<String, Integer>> replicas = this.getStoreReplicas(nearest_node.getElement0());

        System.out.println("REPLICAS: " + replicas.stream().map(Pair::getElement0).collect(Collectors.toList()));


        boolean success = true;

        // If I'm a replica of the store that owns the file or the owner, delete it
        if (nearest_node.getElement0().equals(this.storeIp) ||
                replicas.contains(new Pair<>(this.storeIp, this.storePort))) {
            if (searchDirectory(filekey).equals(MessageCodes.FILE_NOT_FOUND))
                return MessageCodes.FILE_NOT_FOUND;
            deleteFile(filekey);
            // If the request is not from a test client, we don't need to alert the other replicas
            if (!isTestClient) return MessageCodes.DELETE_SUCCESS;
        }

        replicas.add(nearest_node); // send the request to the owner and its replicas
        for (Pair<String, Integer> replica : replicas) {
            if (replica.getElement0().equals(this.storeIp)) continue; // cannot loop the request to myself
            String response_string = this.sendDeleteRequest(new Message("delete", false,
                    this.storeIp, this.storePort, filekey), replica);
            if (!wasDeleteSuccessfull(response_string)) success = false;
        }

        return success ? MessageCodes.DELETE_SUCCESS : MessageCodes.DELETE_FAIL;

    }


    public String put(String filekey, String value, boolean isTestClient) {

        Pair<String, Integer> nearest_node = this.getNearestNodeForKey(filekey);
        List<Pair<String, Integer>> replicas = this.getStoreReplicas(nearest_node.getElement0());


        System.out.println("REPLICAS: " + replicas.stream().map(Pair::getElement0).collect(Collectors.toList()));

        boolean success = true;

        // If I'm a replica of the store that owns the file or the owner, delete it
        if (nearest_node.getElement0().equals(this.storeIp) ||
                replicas.contains(new Pair<>(this.storeIp, this.storePort))) {
            //TODO: Doubt, should I continue even if I can't save it myself if I'm the owner?
            // Should I signal for a Rollback?
            // TRY AGAIN IF FAILURE?

            String saveStatus = saveFile(filekey, value);
            if (saveStatus.equals(MessageCodes.ERROR_SAVING_FILE)) success = false;
            // if(saveStatus.equals(MessageCodes.FILE_EXISTS)) return MessageCodes.FILE_EXISTS;
            if (!isTestClient) return MessageCodes.PUT_SUCCESS;
        }

        replicas.add(nearest_node); // send the request to the owner and its replicas
        for (Pair<String, Integer> replica : replicas) {
            if (replica.getElement0().equals(this.storeIp)) continue; // cannot loop the request to myself
            String response_string = this.sendPutRequest(new Message("put", false,
                    storeIp, storePort, filekey + '\n' + value), replica);
            if (!wasPutSuccessful(response_string)) success = false;
        }

        return success ? MessageCodes.PUT_SUCCESS : MessageCodes.ERROR_CONNECTING;

    }


    /*
             ___       __ _                 _
        | _ \ ___ / _` | _  _  ___  ___| |_  ___
        |   // -_)\__. || || |/ -_)(_-/|  _|(_-/
        |_|_\\___|   |_| \_._|\___|/__/ \__|/__/

     */


    public boolean wasGetSuccessful(String responseString) {
        return responseString != null && !responseString.equals(MessageCodes.ERROR_CONNECTING)
                && !responseString.equals(MessageCodes.FILE_NOT_FOUND) &&
                !responseString.equals(MessageCodes.GET_FAIL);
    }


    public boolean wasPutSuccessful(String responseString) {
        return responseString != null && !responseString.equals(MessageCodes.ERROR_CONNECTING)
                && !responseString.equals(MessageCodes.FILE_NOT_FOUND) &&
                !responseString.equals(MessageCodes.PUT_FAIL);
        // return responseString != null && responseString.equals(MessageCodes.PUT_SUCCESS);
    }

    public boolean wasDeleteSuccessfull(String responseString) {
        return responseString != null && !responseString.equals(MessageCodes.ERROR_CONNECTING)
                && !responseString.equals(MessageCodes.FILE_NOT_FOUND) &&
                !responseString.equals(MessageCodes.DELETE_FAIL);
        // return responseString != null && responseString.equals(MessageCodes.DELETE_SUCCESS);
    }


    String sendGetRequest(Message request_message, Pair<String, Integer> node) {
        Message response_message = sendMessageAndWaitResponse(request_message, node);
        if (response_message == null) return MessageCodes.ERROR_CONNECTING;

        String response_body = new GetMessageBodyParser(response_message.getBody()).parse();
        if (!response_body.equals(MessageCodes.GET_FAIL)) return response_body; //TODO: should FILE_EXISTS be a success
        return MessageCodes.FILE_NOT_FOUND;
    }

    private String sendPutRequest(Message request_message, Pair<String, Integer> store) {
        Message response_message = sendMessageAndWaitResponse(request_message, store);
        if (response_message == null) return MessageCodes.ERROR_CONNECTING;

        String response_body = new PutMessageBodyParser(response_message.getBody()).parseResponseToRequest();
        if (response_body.equals(MessageCodes.PUT_SUCCESS)) // || response_body.equals(MessageCodes.FILE_EXISTS))
            return response_body;
        return MessageCodes.PUT_FAIL;
    }

    String sendDeleteRequest(Message request_message, Pair<String, Integer> node) {
        Message response_message = sendMessageAndWaitResponse(request_message, node);
        if (response_message == null) return MessageCodes.ERROR_CONNECTING;

        String response_body = new DeleteMessageBodyParser(response_message.getBody()).parseResponseToRequest();
        if (response_body.equals(MessageCodes.DELETE_SUCCESS)) return response_body;
        return MessageCodes.FILE_NOT_FOUND;
    }


    Message sendMessageAndWaitResponse(Message message, Pair<String, Integer> node) {
        Socket socket;
        socket = this.sendMessage(message, node);
        if (socket == null) return null;
        Message response = this.getMessage(socket);
        try {
            socket.close();
        } catch (IOException ignored) {
        }
        return response;
    }

    private Socket sendMessage(Message request_message, Pair<String, Integer> storeInfo) {
        String nodeIp = storeInfo.getElement0();
        int nodePort = this.storePort;

        System.out.println("Sending " + request_message.getOperation() + " message to " + nodeIp + ":" + nodePort);
        try {
            Socket socket = new Socket(nodeIp, nodePort); //TODO: Should this be a new thread?
            socket.setSoTimeout(10000);
            SocketsIo.sendStringToSocket(request_message.toString(), socket);
            return socket;
        } catch (IOException e) {
            System.out.println("Error connecting to " + nodeIp + ":" + nodePort);
            return null;
        }
    }


    private Message getMessage(Socket socket) {
        try {
            socket.setSoTimeout(10000);
            String messageString = SocketsIo.readFromSocket(socket);
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
        byte[] encoded;
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
            boolean fileExists = new File(this.folderLocation + "/" + filekey).exists();
            if (fileExists) return MessageCodes.FILE_EXISTS;

            boolean wasDeleted = new File(this.folderLocation + "/" + filekey + ".deleted").exists();
            if (wasDeleted) {
                if (!(new File(this.folderLocation + "/" + filekey + ".deleted").
                        renameTo(new File(this.folderLocation + "/" + filekey))))
                    return MessageCodes.DELETE_FAIL;
            } else Files.write(Paths.get(this.folderLocation + "/" + filekey),
                    value.getBytes(), StandardOpenOption.CREATE);

            return MessageCodes.PUT_SUCCESS;
        } catch (IOException e) {
            e.printStackTrace();
            //TODO: RETRY 2 more times
            return MessageCodes.ERROR_SAVING_FILE;
        }
    }


    ArrayList<String> getNonDeletedFiles() {
        ArrayList<String> files = new ArrayList<>();
        File folder = new File(this.folderLocation);
        for (File file : Objects.requireNonNull(folder.listFiles())) {
            if (file.getName().endsWith(".deleted") || file.getName().equals("membership_log.txt")) continue;
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
        // System.out.println("NODE POS: " + index);

        for (int i = 1; i <= 2; ++i) {
            // if (i < 0) i = sortedCluster.size() - 1;
            int replica_index = (index - i) % sortedCluster.size();
            if (replica_index < 0) replica_index += sortedCluster.size();
            // TODO: if(replica_index == index) continue;
            ArrayList<String> replica_node = sortedCluster.get(replica_index);
            replicas.add(new Pair<>(replica_node.get(0), this.storePort));
        }

        // System.out.println("FOUND REPLICAS: " + replicas);

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

        ArrayList<String> files = this.getNonDeletedFiles();
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
        int index = Collections.binarySearch(hash_values, filekey);


        if (index < 0) {
            index = -index - 1; // revert the negative index
            if (index == availableNodes.size()) index = 0; // if it overflows, set it to the first node
        }

        return index;
    }


    private ArrayList<String> getPredecessors(String nodeIp) {
        ArrayList<String> replicas = getStoreReplicas(nodeIp).stream().
                map(Pair::getElement0).distinct().collect(Collectors.toCollection(ArrayList::new));
        replicas.remove(nodeIp);
        return replicas;
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


        List<String> filtered = replicas.stream().map(Pair::getElement0).distinct().collect(Collectors.toList());
        filtered.remove(storeIp);
        return filtered;
    }

    private List<String> getPreferenceList(String filekey) {

        List<String> preference_list = new ArrayList<>();

        Pair<String, Integer> nearestNodeForKey = this.getNearestNodeForKey(filekey);
        String nearestNodeIp = nearestNodeForKey.getElement0();

        preference_list.add(nearestNodeIp);
        preference_list.addAll(this.getPredecessors(nearestNodeIp));
        return preference_list;
    }

    private List<String> getFilesThatIOwn() {
        List<String> storedFiles = this.getNonDeletedFiles();
        List<String> myFiles = new ArrayList<>();

        for (String file : storedFiles) {
            Pair<String, Integer> nearestNodeForKey = this.getNearestNodeForKey(file);
            if (nearestNodeForKey.getElement0().equals(this.storeIp)) myFiles.add(file);
        }
        return myFiles;
    }


    private String purgeFiles() {
        // for each non deleted file, check if my storeIp is in the preference list
        List<String> myFiles = this.getNonDeletedFiles();
        for (String file : myFiles) {
            List<String> preferenceList = this.getPreferenceList(file);
            if (!preferenceList.contains(this.storeIp)) this.deleteFile(file);
        }

        return MessageCodes.DELETE_SUCCESS;
    }


    public String updateAndBroadCastClusterInfo(String newStoreIp){
        // update cluster
        //Add store to cluster
        this.updateStoreToCluster(newStoreIp, Store.UPDATE_CLUSTER_JOIN);

        //Compose body (List of clusterMembers and last32logs)
        String body = this.convertMembershipToString(false);

        // SEND INFO THE NODE THAT MADE THE MULTICAST REQUEST THROUGH TCP
        Message send = new Message("membership", false, this.getStoreIp(), this.storePort, body );

        try {
            Socket socket = new Socket(newStoreIp, this.storePort);
            SocketsIo.sendStringToSocket( send.toString(), socket );
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
            return MessageCodes.UPDATE_CLUSTER_FAIL;
        }

        return MessageCodes.UPDATE_CLUSTER_SUCCESS;
    }


    public String membershipJoinHandler(String newStoreIp) {
        // get files for which I'm the owner before the join entry
        List<String> files = this.getFilesThatIOwn();

        if(this.updateAndBroadCastClusterInfo(newStoreIp).equals(MessageCodes.UPDATE_CLUSTER_FAIL))
            return MessageCodes.UPDATE_CLUSTER_FAIL;;

        // get successors
        List<String> successors = this.getSucessors(newStoreIp);
        //System.out.println("Successors: " + successors);

        // get predecessors
        List<String> predecessors = this.getPredecessors(newStoreIp);
        // System.out.println("Predecessors: " + predecessors);

        // If I am not successor and not a predecessor, leave
        if (!successors.contains(this.storeIp) && !predecessors.contains(this.storeIp))
            return MessageCodes.UPDATED_WITH_JOIN;

        // If I am successor send files I own/owned
        if (successors.contains(this.storeIp)) {
            for (String file : files) {
                String value = this.searchDirectory(file);
                Message message = new Message(MessageType.PUT.toString(), false,
                        this.storeIp, this.storePort, file + "\n" + value);

                System.out.println("Sending file: " + file + " to new node: " + newStoreIp);
                sendPutRequest(message, Pair.createPair(newStoreIp, this.storePort));
            }
        }

        // clear files that I don't own anymore
        this.purgeFiles();

        return MessageCodes.UPDATED_WITH_JOIN;
    }


    /**
     * Stores enters the cluster
     * Calls a thread in charge of the joining process
     */
    public void join() {

        MembershipProtocolJoin server = new MembershipProtocolJoin(this);
        Thread thread = new Thread(server);
        thread.start();

    }

    /**
     * Starts the multicast udp cluster server
     */
    public void startUdpServer() {
        this.udpClusterServer = new Thread(new StoreUdpServer(this, clusterIp, clusterPort));

        udpClusterServer.start();
    }

    /**
     * Interrupts the Udp server
     */
    public void closeUdpServer() {
        this.udpClusterServer.interrupt();
    }

    /**
     * In case no store is in the cluster, this method is in charge of initialize the membership
     * not only starting the multicast server but also stating it is joining
     */
    public void initializeMembership() {
        this.startUdpServer();

        //Entered the membership, start the Udp serverSocket

        System.out.println("This is the first Membership Store");

        updateStoreToCluster(this.storeIp, Store.UPDATE_CLUSTER_JOIN);

    }


    /**
     * This store is trying to leave the cluster.
     * It removes itself from the cluster, sends the leave message and closes Udp server
     */
    public void leave() {
        this.updateStoreToCluster(this.storeIp, Store.UPDATE_CLUSTER_LEAVE);
        this.stopSendingPeriodicMembership();

        InetAddress ip_adressCluster;
        try {
            ip_adressCluster = InetAddress.getByName(this.clusterIp);
            DatagramSocket udpSocket = new DatagramSocket();
            //Create message to send
            Message newMessage = new Message("leave", false, this.storeIp, this.storePort, "");
            SocketsIo.sendUdpMessage(newMessage, udpSocket, ip_adressCluster, this.clusterPort);
            closeUdpServer();

            //Percorrer as suas keys
            ArrayList<String> keys = getNonDeletedFiles();

            for(String key: keys){
                String fileContent = searchDirectory(key);
                this.put(key, fileContent, true);
                this.deleteFile(key);

            }


        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    /**
     * This fucntion sends an Udp Message with membership
     */
    public void sendPeriodicMembership() {
        String body = this.convertMembershipToString(false);

        Message send = new Message("membership", false, this.storeIp, this.storePort, body);

        try {
            DatagramSocket udpSocket = new DatagramSocket();
            InetAddress address = InetAddress.getByName(this.clusterIp);
            SocketsIo.sendUdpMessage(send, udpSocket, address, this.clusterPort);

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    /**
     * Starts sending periodic Membership messages
     */
    public void startSendingPeriodicMembership() {
        if (this.periodicMembershipSender == null) {
            this.periodicMembershipSender = new ScheduledThreadPoolExecutor(1);
            periodicMembershipSender.scheduleAtFixedRate(() -> sendPeriodicMembership(), 0, 15, TimeUnit.SECONDS);
        }
    }

    /**
     * Stops sending periodic Membership messages
     */
    public void stopSendingPeriodicMembership() {
        this.periodicMembershipSender.shutdown();
        this.periodicMembershipSender = null;
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
    Message format:
    List:
    ip counter
    ip counter
    ip counter

    Logs:
    ip counter
    ip counter
    ip counter


    */
    public String convertMembershipToString(boolean withClusterList) {
        String body = "";

        if (withClusterList) {
            body += "List:\n";

            for (ArrayList<String> list : this.cluster) {
                body += list.get(0) + " " + list.get(1) + "\n";
            }

        }

        body += "Logs:\n";

        for (Object log : this.last32Logs.toArray()) {
            body += log.toString() + "\n";

        }

        return body;
    }

}
