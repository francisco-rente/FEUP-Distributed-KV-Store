package distributed_system_project;

import distributed_system_project.message.Message;
import distributed_system_project.message.body_parsers.DeleteMessageBodyParser;
import distributed_system_project.message.body_parsers.GetMessageBodyParser;
import distributed_system_project.utilities.Pair;
import distributed_system_project.utilities.ShaHasher;
import distributed_system_project.utilities.SocketsIo;

import java.io.*;
import java.net.*;
import java.nio.channels.ServerSocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.security.NoSuchAlgorithmException;
import java.util.*;


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

        try {
            this.storeId = Encoder.encryptSHA(storeIp);
        } catch (NoSuchAlgorithmException | UnsupportedEncodingException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

        //Creates Store Id


        this.folderLocation = "./node_db/" + storeId;
        this.membershipLog = this.folderLocation + "/membership_log.txt";

        this.cluster = new ArrayList<>();
        this.cluster.add(new ArrayList<>(Arrays.asList(storeIp, String.valueOf(storePort), STARTING_MEMBERSHIP_COUNTER)));


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
                new ArrayList<>(this.cluster.stream().filter(node -> Integer.parseInt(node.get(2)) % 2 == 0).toList());

        System.out.println("Available nodes: " + availableNodes);

        // sort available nodes ip value using a lambda that uses compareTo
        availableNodes.sort(Comparator.comparing((ArrayList<String> node) -> ShaHasher.getHashString(node.get(0))));

        List<String> node_ips = availableNodes.stream().map(node -> node.get(0)).toList();
        List<String> hash_values = node_ips.stream().map(ShaHasher::getHashString).toList();

        // print hashed values of nodes ip
        System.out.println("Hashed values of nodes ip: " + hash_values);
        int index = Collections.binarySearch(hash_values, filekey);

        System.out.println("Binary search index: " + index);

        if (index < 0) {
            index = -index - 1; // revert the negative index
            if (index == availableNodes.size()) index = 0; // if it overflows, set it to the first node
        }

        System.out.println("Store Index for the operation: " + index + "\n");

        nearest_node_ip = availableNodes.get(index).get(0);
        nearest_node_port = Integer.parseInt(availableNodes.get(index).get(1));
        return Pair.createPair(nearest_node_ip, nearest_node_port);
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

}
