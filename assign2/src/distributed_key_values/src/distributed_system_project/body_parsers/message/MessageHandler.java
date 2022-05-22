package distributed_system_project.body_parsers.message;

import distributed_system_project.body_parsers.message.body_parsers.DeleteMessageBodyParser;
import distributed_system_project.body_parsers.message.body_parsers.GetMessageBodyParser;
import distributed_system_project.body_parsers.utilities.Pair;
import distributed_system_project.body_parsers.Store;
import distributed_system_project.body_parsers.message.body_parsers.PutMessageBodyParser;

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
    private final Socket socket;

    public MessageHandler(Store store, Socket socket) {
        this.store = store;
        this.socket = socket;
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
        String status = this.store.put(keyValuePair.getElement0(), keyValuePair.getElement1());

        Message response = new Message("put", false, message.getIp(), message.getPort(),
                status == null ? "ERROR: File not found" : status);

        sendMessageToSocket(response);
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

    }

    public void handleLeaveOperation(Message message) {

    }

    @Override
    public void run() {

        try {
            InputStream input = this.socket.getInputStream();
            BufferedReader reader = new BufferedReader(new InputStreamReader(input));
            StringBuilder messageString = new StringBuilder();

            // read until the end of the stream
            String line;
            while ((line = reader.readLine()) != null) {
                // concat the line to the message
                messageString.append(line);
            }

            this.message = Message.toObject(messageString.toString());

            MessageType type = MessageType.getMessageType(message, this.store);
            System.out.println(message);
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
    }
}