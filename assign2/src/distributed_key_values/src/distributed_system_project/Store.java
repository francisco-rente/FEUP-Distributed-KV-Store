package distributed_system_project;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
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
import distributed_system_project.message.body_parsers.DeleteMessageBodyParser;
import distributed_system_project.message.body_parsers.GetMessageBodyParser;
import distributed_system_project.utilities.Pair;
import distributed_system_project.utilities.ShaHasher;
import distributed_system_project.utilities.SocketsIo;


public class Store {

    private static final String STARTING_MEMBERSHIP_COUNTER = "0";
    private static final int MEMBERSHIP_PORT = 7777;
    private static final int TIMEOUT_TIME = 10000;

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
        if(this.getMembershipCounter()%2 ==0){
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
    private void readClusterInformationFromLog(){
        File file = new File(this.membershipLog);

        try {
            if(!file.createNewFile()){
                FileInputStream fis = new FileInputStream(file);
                BufferedReader br = new BufferedReader(new InputStreamReader(fis));
                for(String tmp; (tmp = br.readLine()) != null;){
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
    private void createClusterFromLogs(){

        for(String log: this.last32Logs){
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
    private void writeLogs(){
        FileSystem.writeTextOnFile(this.membershipLog, this.last32Logs);
    }
    
    /**
     * Adds a new membership log to the last32Logs
     * @param last32Logs
     */
    public void setLast32Logs(List<String> last32Logs) {

        for(String log : this.last32Logs){
            this.addLog(log);
        }
    }

    /**
     * Adds a log to the last32Logs
     * In case a log of such store ip already exists, it is replaced
     * This function changes the membership log and cluster
     * @param log
     */
    public void addLog(String log) {  

        boolean changedLog = false;
        String[] logInfo = log.split(" "); 
        for(int i = 0; i< this.last32Logs.size(); i++){
            String[] tmpInfo = this.last32Logs.get(i).split(" ");

            //If exists already on list, ends here
            if(log.equals(this.last32Logs.get(i))){return;}

            //If already has info on list but membership is different
            if(logInfo[0].equals(tmpInfo[0]) && !logInfo[1].equals(tmpInfo[1])){
                String newLog = logInfo[0] + " " + Math.max(Integer.parseInt(tmpInfo[1]), Integer.parseInt(logInfo[1]));   
                this.last32Logs.set(i, newLog);
                changedLog = true;
                  
                break;
            }

        }

        //If not there, adds to the log
        if(!changedLog)last32Logs.add(log);
        this.createClusterFromLogs();
        this.writeLogs();
    }


    public int getMembershipCounter(){
        int count = -1;

        for(String log : this.last32Logs){
            List<String> tmpLog = FileSystem.logToInfo(log);
            if(tmpLog.get(0).equals(this.storeIp)){
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

    public void setClusterNodes( List<ArrayList<String>> cluster ) {
        this.cluster = cluster;
    }

    /**
     * Updates the mmebership information of the store which ip is storeIp
     * @param storeIp The ip address of the store
     * @param joinOrLeave If the store is entering or leaving the cluster. If variable is 0 it is joining, if it is 1 it is leaving
     */
    public void updateStoreToCluster(String storeIp, int joinOrLeave) {
        
        for(int i = 0; i< last32Logs.size(); i++){
            ArrayList<String> tmpLog = FileSystem.logToInfo(last32Logs.get(i));

            if(tmpLog.get(0). equals(storeIp)){
                if(Integer.parseInt(tmpLog.get(1))%2 != joinOrLeave){
                    String newLog = tmpLog.get(0) + " " + String.valueOf(Integer.parseInt(tmpLog.get(1))+1);
                    addLog(newLog);
                }

                return;
            }
        }

        //Case of joining the cluster of the first time
        if(joinOrLeave == 0){
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


    public String get(String filekey) {
        try {
            if (new File(this.folderLocation + "/" + filekey).exists()) {

                byte[] encoded = Files.readAllBytes(Paths.get(this.folderLocation + "/" + filekey));
                return new String(encoded, StandardCharsets.UTF_8);

            } else {

                Pair<String, Integer> nearest_node = this.getNearestNode(filekey);

                if (nearest_node.getElement0().isEmpty()) throw new FileNotFoundException();

                Message request_message = new Message("get", false,
                        nearest_node.getElement0(), nearest_node.getElement1(), filekey);

                Socket socket = this.sendMessage(request_message);
                Message response_message = this.getMessage(socket);

                GetMessageBodyParser parser = new GetMessageBodyParser(response_message.getBody());
                return parser.parse();
            }
        } catch (IOException e) {
            e.printStackTrace();
            return "FILE_NOT_FOUND_ERROR";
        }
    }


    String searchDirectory(String filekey) throws IOException {
        if (!new File(this.folderLocation + "/" + filekey).exists()) return null;
        byte[] encoded = Files.readAllBytes(Paths.get(this.folderLocation + "/" + filekey));

        //TODO: Does something have to happen for tombstones?

        return new String(encoded, StandardCharsets.UTF_8);
    }

    boolean deleteFile(String filekey) {
        File file = new File(this.folderLocation + "/" + filekey);
        return file.renameTo(new File(this.folderLocation + "/" + filekey + ".deleted"));
    }


    public String delete(String filekey) {
        try {
            if (searchDirectory(filekey) != null) return (deleteFile(filekey)) ? "FILE_DELETED" : "FILE_DELETE_ERROR";

            Pair<String, Integer> nearest_node = this.getNearestNode(filekey);
            if (nearest_node.getElement0().isEmpty()) throw new FileNotFoundException();

            Message request_message = new Message("delete", false,
                    nearest_node.getElement0(), nearest_node.getElement1(), filekey);
            this.sendMessage(request_message);

            Socket socket = this.sendMessage(request_message);
            Message response_message = this.getMessage(socket);

            DeleteMessageBodyParser parser = new DeleteMessageBodyParser(response_message.getBody());
            return parser.parse();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return "FILE_NOT_FOUND_ERROR";
        } catch (IOException e) {
            e.printStackTrace();
            return "FILE_DELETE_ERROR";
        }
    }

    private List<Pair<String, Integer>> getNearestNodesWithFile(String filekey) {
        List<Pair<String, Integer>> nearest_nodes = new ArrayList<>();
        return nearest_nodes;
    }

    public String put(String filekey, String value) throws IOException {

        Pair<String, Integer> nearest_node = this.getNearestNode(filekey);

        if (nearest_node.getElement0().isEmpty()) throw new FileNotFoundException("No nearest node found");

        if (Objects.equals(nearest_node.getElement0(), this.storeIp)) {
            boolean file_exists = new File(this.folderLocation + "/" + filekey).exists();
            if (file_exists) return "ERROR";
            Files.write(Paths.get(this.folderLocation + "/" + filekey), value.getBytes(), StandardOpenOption.CREATE);
            return "SUCCESS";
        } else {
            System.out.println("Redirect put request to " + nearest_node.getElement0() + ":" + nearest_node.getElement1());

            // send the file to the nearest node
            Message request_message = new Message("put", false,
                    nearest_node.getElement0(), nearest_node.getElement1(), filekey + '\n' + value);
            Socket socket = this.sendMessage(request_message);

            Message response = this.getMessage(socket);
            return response.getBody().equals("ERROR") ? "ERROR" : "SUCCESS";
        }
    }


    private Socket sendMessage(Message request_message) throws IOException {
        String nodeIp = request_message.getIp();
        int nodePort = request_message.getPort();

        System.out.println("Sending" + request_message.getOperation() + " message to " + nodeIp + ":" + nodePort);
        Socket socket = new Socket(nodeIp, nodePort);

        SocketsIo.sendStringToSocket(request_message.toString(), socket);
        return socket;
    }


    private Message getMessage(Socket socket) throws IOException {
        String messageString = SocketsIo.readFromSocket(socket);
        assert messageString != null;
        return Message.toObject(messageString);
    }


    private Pair<String, Integer> getNearestNode(String filekey) {
        String nearest_node_ip;
        int nearest_node_port;

        // get nodes with active counter
        List<ArrayList<String>> availableNodes =
                new ArrayList<>(this.cluster.stream().filter(node -> Integer.parseInt(node.get(2)) % 2 == 0).collect(Collectors.toList()));

        System.out.println("Available nodes: " + availableNodes);

        // sort available nodes ip value using a lambda that uses compareTo
        availableNodes.sort(Comparator.comparing((ArrayList<String> node) -> ShaHasher.getHashString(node.get(0))));

        List<String> node_ips = availableNodes.stream().map(node -> node.get(0)).collect(Collectors.toList());

        // print hashed values of nodes ip
        System.out.println("Hashed values of nodes ip: " + node_ips.stream().map(ShaHasher::getHashString).collect(Collectors.toList()));

        int index = Collections.binarySearch(node_ips, filekey, Comparator.comparing(ShaHasher::getHashString));

        System.out.println("Binary search index: " + index);

        if (index < 0) {
            index = -index - 1; // revert the negative index
            if (index == availableNodes.size()) index = 0; // if it overflows, set it to the first node
        }

        System.out.println("Store Index for the operation: " + index + "\n");

        nearest_node_ip = this.cluster.get(index).get(0);
        nearest_node_port = Integer.parseInt(this.cluster.get(index).get(1));
        return Pair.createPair(nearest_node_ip, nearest_node_port);
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
    public void startUdpServer(){
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
    public void leave(){
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
            
            
        } catch (UnknownHostException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (SocketException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    /**
     * This fucntion sends an Udp Message with membership
     */
    public void sendPeriodicMembership(){
        String body = this.convertMembershipToString(false);
        
        Message send = new Message("membership", false, this.storeIp,this.storePort ,body );
        
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
    public void startSendingPeriodicMembership(){
        if(this.periodicMembershipSender == null){
            this.periodicMembershipSender = new ScheduledThreadPoolExecutor(1);
            periodicMembershipSender.scheduleAtFixedRate(() -> sendPeriodicMembership(), 0, 60, TimeUnit.SECONDS);
        }


    }

    /**
     * Stops sending periodic Membership messages
     */
    public void stopSendingPeriodicMembership(){
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

        while (true) {}
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
    public String convertMembershipToString(boolean withClusterList){
        String body = "";

        if(withClusterList){
            body += "List:\n";

            for(ArrayList<String> list: this.cluster){
                body += list.get(0) + " " + list.get(1) +"\n";
            }

        }

        body += "Logs:\n";

        for(Object log : this.last32Logs.toArray()){
            body += log.toString() + "\n";

        }

        return body;
    }

}
