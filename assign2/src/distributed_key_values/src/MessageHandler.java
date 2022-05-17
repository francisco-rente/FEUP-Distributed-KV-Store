import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
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
        // body has key
        String key = message.getBody();

        // obtain value from store
        String value = this.store.get(key);

        Message response;
        if (value != null) {
            response = new Message("get", false, message.getIp(), message.getPort(), value);
        } else {
            response = new Message("get", false, message.getIp(), message.getPort(), "ERROR: File not found");
        }

        // create output stream
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

    public void handlePutOperation(Message message) {
        // get key value pair from body  
        // ArrayList<String> keyValuePair = message.getBody(PUT_BODY);

        // put api call 
    }

    public void handleDeleteOperation(Message message) {
        
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
                    this.handleGetOperation();
                    break;
                case PUT:
                    this.handlePutOperation();
                    break;
                case DELETE:
                    this.handleDeleteOperation();
                    break;
                case JOIN:
                    this.handleJoinOperation();
                    break;
                case LEAVE:
                    this.handleLeaveOperation();
                    break;
                case UNKNOWN:
                    break;

            
            System.out.println(message);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

}