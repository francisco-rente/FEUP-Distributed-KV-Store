import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Array;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;


import java.net.InetAddress;



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
        this.last32Logs = new PriorityQueue<String>();

        try {
            this.storeId = Encoder.encryptSHA(storeIp);
        } catch (NoSuchAlgorithmException | UnsupportedEncodingException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

        //Creates Store Id
        

        this.folderLocation = "./node_db/" + storeId; 
        this.membershipLog = this.folderLocation + "/membership_log.txt"; 

        this.cluster = new ArrayList<ArrayList<String>>(); 

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

    public void addLog(String log){
        if(this.last32Logs.size()==32){
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

    public StoreUdpServer getUdpServer(){
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

    public void addStoreToCluster(String storeIp, String membershipCounter){

        for(ArrayList<String> list : this.cluster){
            if(list.get(0).equals(storeIp)){
                return;
            }
        }

        ArrayList<String> store = new ArrayList<String>();
        store.add(storeIp);
        store.add(membershipCounter);


        this.cluster.add(store);
        String log =  storeIp + " " + membershipCounter;
        addLog(log);

        FileSystem.writeOnFile(this.membershipLog , log);


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


    public String get(String filekey) {

        // read file content using Scanner
        Scanner scanner;
        try {
            scanner = new Scanner(new File(this.folderLocation + "/" + filekey));
            String bodyString = "";
            while (scanner.hasNextLine()) {
                bodyString += scanner.nextLine();
            }
        scanner.close();
        return bodyString;
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return "ERROR";
        }
    }

    

    public void join(){

        this.udpClusterServer = new StoreUdpServer(this, clusterIp, clusterPort);
        Thread udpServer = new Thread(this.udpClusterServer);
        udpServer.start();

        MembershipProtocolJoin server = new MembershipProtocolJoin(this);
        Thread thread = new Thread(server);

        thread.start();

    }

    public void initializeMembership(){
        System.out.println("This is the first Membership Store");

        ArrayList<String> storeInfo = new ArrayList<String>();
        storeInfo.add(storeIp);
        storeInfo.add(Store.STARTING_MEMBERSHIP_COUNTER);

        this.cluster.add( storeInfo);

        String log = storeIp + " " + Store.STARTING_MEMBERSHIP_COUNTER;
        addLog(log);
        FileSystem.writeOnFile(membershipLog, log);

        
    }


    public static void main(String[] args) {
        if(args.length!=4 ){
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

        while(true){
        
        }
        
    }

}
