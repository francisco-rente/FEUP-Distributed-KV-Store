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
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;

public class MessageHandlerForAsyncSocket implements Runnable {
    private static final int READ_BUFFER_MAX_SIZE = 2048;
    private final Store store;
    private Message message;
    private final AsynchronousSocketChannel socket;

    public MessageHandlerForAsyncSocket(Store store, AsynchronousSocketChannel socket) {
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
        String status = null;
        try {
            status = this.store.put(keyValuePair.getElement0(), keyValuePair.getElement1());
        } catch (IOException e) {
            e.printStackTrace();
        }

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


    public void handleJoinOperation(Message message) {

    }

    public void handleLeaveOperation(Message message) {

    }

    @Override
    public void run() {
        String messageString = getMessageString();

        this.message = Message.toObject(messageString.toString());

        MessageType type = MessageType.getMessageType(message, this.store);
        handleMessage(type);
    }



    private void sendMessageToSocket(Message response) {
        byte[] response_bytes = response.toString().getBytes();
        ByteBuffer buffer = ByteBuffer.wrap(response_bytes);
        socket.write(buffer, null, new CompletionHandler<Integer, Void>() {
            @Override
            public void completed(Integer result, Void attachment) {
                System.out.printf("Sent %d bytes", result);

                if(buffer.hasRemaining()) {
                    socket.write(buffer, null, this);
                }
                else if(result == -1) {
                    System.out.println("Error sending message");
                    throw new RuntimeException("Error sending message");
                }
                else{
                    System.out.println("Message sent");
                }
            }

            @Override
            public void failed(Throwable exc, Void attachment) {
                System.out.println("Message sending failed");
            }
        });

    }



    private void handleMessage(MessageType type) {
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

    }
    private String getMessageString(){

        // read message from socket with CompletionHandler
        ByteBuffer buffer = ByteBuffer.allocate(READ_BUFFER_MAX_SIZE);
        socket.read(buffer, null, new CompletionHandler<Integer, Void>() {
            @Override
            public void completed(Integer result, Void attachment) {
                System.out.printf("Read %d bytes from socket\n", result);
                if(buffer.remaining() > 0) {
                    socket.read(buffer, null, this);
                }
                else {
                    System.out.println("Message received");
                }
            }

            @Override
            public void failed(Throwable exc, Void attachment) {
                System.out.println("Message receiving failed");
            }
        });

        byte[] bytes = new byte[buffer.position()];
        buffer.rewind();
        buffer.get(bytes);
        return new String(bytes);
    }
}