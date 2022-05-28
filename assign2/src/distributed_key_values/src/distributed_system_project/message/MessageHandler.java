package distributed_system_project.message;

import distributed_system_project.message.body_parsers.DeleteMessageBodyParser;
import distributed_system_project.message.body_parsers.GetMessageBodyParser;
import distributed_system_project.utilities.Pair;
import distributed_system_project.Store;
import distributed_system_project.message.body_parsers.PutMessageBodyParser;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.IOException;
import java.net.Socket;

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
        this.store = store;
        this.message = message;
        this.isTcp = false;
    }

    public void handleGetOperation(Message message) {
        GetMessageBodyParser body_parser = new GetMessageBodyParser(message.getBody());

        String key = body_parser.parse();

        // obtain value from store or from other nodes
        String value = this.store.get(key);

        Message response = new Message("get", false, message.getIp(), message.getPort(),
                (value == null) ? "ERROR: File not found" : value);

        sendMessageToSocket(response);
    }

    public void handlePutOperation(Message message) {
        PutMessageBodyParser putMessageBodyParser = new PutMessageBodyParser(message.getBody());
        Pair<String, String> keyValuePair = putMessageBodyParser.parse();

        // store the value in the store or in other nodes (if the key is adequate)
        // String status = this.store.put(keyValuePair.getElement0(), keyValuePair.getElement1());

        /*Message response = new Message("put", false, message.getIp(), message.getPort(),
                status == null ? "ERROR: File not found" : status);*/

        //sendMessageToSocket(response);
    }

    public void handleDeleteOperation(Message message) {
        DeleteMessageBodyParser deleteMessageBodyParser = new DeleteMessageBodyParser(message.getBody());
        String key = deleteMessageBodyParser.parse();

        // tombstone the value in the store or in other nodes
        String status = this.store.delete(key);

        Message response = new Message("delete", false, message.getIp(),
                message.getPort(), status == null ? "ERROR: PUT OPERATION UNSUCCESSFUL" : status);

        sendMessageToSocket(response);
    }

    private void sendMessageToSocket(Message response) {
        OutputStream outputStream;
        try {
            outputStream = this.socket.getOutputStream();
            PrintWriter printWriter = new PrintWriter(outputStream, true);
            printWriter.println(response.toString());
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public void handleJoinOperation(Message message) {
        //Create socket to send message
        if(message.isTestClient()){
            store.join();

        }else{

            System.out.println(message.toString());
            //Add store to cluster
            this.store.addStoreToCluster(message.getIp(), Store.getStartingMembershipCounter());

            String body = "";



            Message send = new Message("membership", false, message.getIp(), message.getPort(), body );


        }

    }

    public void handleLeaveOperation(Message message) {

    }

    @Override
    public void run() {

        if(this.isTcp){
            try {
                InputStream input = this.socket.getInputStream();
                BufferedReader reader = new BufferedReader(new InputStreamReader(input));
                String messageString = "";

                // read until the end of the stream
                String line = "";
                while ((line = reader.readLine()) != null) {
                    // concat the line to the message
                    messageString += line;
                }

                this.message = Message.toObject(messageString);

                MessageType type = MessageType.getMessageType(message, this.store);
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
                    case UNKNOWN:
                        break;

                }

            } catch (IOException e) {
                e.printStackTrace();
            }
        } else{

            this.handleJoinOperation(this.message);

        }
    }
}