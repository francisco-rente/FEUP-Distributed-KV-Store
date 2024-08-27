package distributed_system_project.message;

import distributed_system_project.message.body_parsers.DeleteMessageBodyParser;
import distributed_system_project.message.body_parsers.GetMessageBodyParser;
import distributed_system_project.message.body_parsers.MembershipBodyParser;
import distributed_system_project.utilities.Pair;
import distributed_system_project.Store;
import distributed_system_project.message.body_parsers.PutMessageBodyParser;
import distributed_system_project.utilities.SocketsIo;

import java.io.IOException;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;



public class MessageHandler implements Runnable {
    private final Store store;
    private Message message;
    private Socket socket;
    private final boolean isTcp;

    //For tcp
    public MessageHandler(Store store, Socket socket) {
        this.store = store;
        this.socket = socket;
        this.isTcp = true;
    }

    //Fpr udp
    public MessageHandler(Store store, Message message){

        System.out.println("RECEIVED MESSAGE: " + message.getOperation() + "\n");

        this.store = store;
        this.message = message;
        this.isTcp = false;
    }

    public void handleGetOperation(Message message) throws IOException {
        GetMessageBodyParser body_parser = new GetMessageBodyParser(message.getBody());

        String key = body_parser.parse();

        System.out.println("-----------------\n");
        System.out.println("Get Request: " + key);
        System.out.println("-----------------\n");

        // obtain value from store or from other nodes
        String value = this.store.get(key, message.isTestClient());

        Message response = new Message("get", false, message.getIp(), message.getPort(),
                (value == null) ? MessageCodes.FILE_NOT_FOUND : value);

        SocketsIo.sendStringToSocket(response.toString(), this.socket);
    }

    public void handlePutOperation(Message message) throws IOException {
        PutMessageBodyParser putMessageBodyParser = new PutMessageBodyParser(message.getBody());
        Pair<String, String> keyValuePair = putMessageBodyParser.parse();


        System.out.println("-----------------\n");
        System.out.println("PUT REQUEST RECEIVED: " + keyValuePair.getElement0() + " " + keyValuePair.getElement1());
        System.out.println("-----------------\n");

        // store the value in the store or in other nodes (if the key is adequate)
        String status = this.store.put(keyValuePair.getElement0(), keyValuePair.getElement1(), message.isTestClient());

        Message response = new Message("put", false, message.getIp(), message.getPort(),
                status == null ? "ERROR: File not found" : status);

        SocketsIo.sendStringToSocket(response.toString(), this.socket);
    }

    public void handleDeleteOperation(Message message) {
        DeleteMessageBodyParser deleteMessageBodyParser = new DeleteMessageBodyParser(message.getBody());
        String key = deleteMessageBodyParser.parse();

        System.out.println("-----------------\n");
        System.out.println("DELETE REQUEST RECEIVED: " + key);
        System.out.println("-----------------\n");

        // tombstone the value in the store or in other nodes
        String status = this.store.delete(key, message.isTestClient());

        Message response = new Message("delete", false, message.getIp(),
                message.getPort(), status);

        SocketsIo.sendStringToSocket(response.toString(), this.socket);
    }


    public void handleJoinOperation(Message message) {
        //Create socket to send message
        if(message.isTestClient()){
            if(this.store.getMembershipCounter() %2 == 0){
                SocketsIo.sendStringToSocket("Already on cluster\nend", socket);
                return;
            }
            store.join();
            SocketsIo.sendStringToSocket("Entering cluster\nend", socket);

        }else{
            this.store.membershipJoinHandler(message.getIp());
        }

    }


    public void handleMembershipOperation(Message message) {
        MembershipBodyParser membershipBodyParser = new MembershipBodyParser(message.getBody());

        Pair<List<String>, List<String>> membership = membershipBodyParser.parse();

        List<ArrayList<String>> members =new ArrayList<>();

        for(String storeString : membership.getElement0()){
            ArrayList<String> storeInfo = new ArrayList<>();
            String[] storeStringInfo = storeString.trim().split(" ");
            storeInfo.add(storeStringInfo[0]);
            storeInfo.add(storeStringInfo[1]);
            
            members.add(storeInfo);
        }

        for(String log: membership.getElement1()){
            this.store.addLog(log);
        }

        this.store.startSendingPeriodicMembership();

    }

    public void handleLeaveOperation(Message message) {
        if(message.isTestClient()){


            Integer counter = Math.abs(this.store.getMembershipCounter());

            if(counter == 1){
                SocketsIo.sendStringToSocket("Already on out of cluster\nend", socket);
                return;
            }
            SocketsIo.sendStringToSocket("Left the cluster\nend", socket);
            this.store.leave();
            //Prepares the leave operation and send leave message
        }
        else{
            //Receives leave message from leaver
            //Atualizar logs e cluster
            this.store.updateStoreToCluster(message.getIp(), Store.UPDATE_CLUSTER_LEAVE);            

        }

    }

    @Override
    public void run() {

        if(this.isTcp){
            System.out.println("TCP");

            try {
                // read all lines of the message until EOF
                String messageString =  SocketsIo.readFromSocket(this.socket);
                // System.out.println("RECEIVED MESSAGE: " + messageString + "\n");

                //assert messageString != null;
                this.message = Message.toObject(messageString);

                MessageType type = MessageType.getMessageType(message);


                // TODO discover header type
                switch (type) {
                    case GET:
                        this.handleGetOperation(this.message);
                        break;
                    case PUT:
                        this.handlePutOperation(this.message);
                        break;
                    case DELETE:
                        this.handleDeleteOperation(this.message);
                        break;
                    case JOIN:
                        this.handleJoinOperation(this.message);
                        break;
                    case LEAVE:
                        this.handleLeaveOperation(this.message);
                        break;
                    case MEMBERSHIP:
                        this.handleMembershipOperation(this.message);
                        break;
                    case UNKNOWN:
                        break;
                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        } else{
            MessageType type = MessageType.getMessageType(message);

            System.out.println("HANDLING OPERATION : " + message.getOperation() + "\n");

            // TODO discover header type
            switch (type) {
                case GET:
                    try {
                        this.handleGetOperation(this.message);
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    break;
                case PUT:
                    try {
                        this.handlePutOperation(this.message);
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                    break;
                case DELETE:
                    this.handleDeleteOperation(this.message);
                    break;
                case JOIN:
                    this.handleJoinOperation(this.message);
                    break;
                case LEAVE:
                    this.handleLeaveOperation(this.message);
                    break;
                case MEMBERSHIP:
                    this.handleMembershipOperation(this.message);
                    break;
                case UNKNOWN:
                    break;
            }
        }
    }



    public void closeSocket(){
        try {
            this.socket.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}