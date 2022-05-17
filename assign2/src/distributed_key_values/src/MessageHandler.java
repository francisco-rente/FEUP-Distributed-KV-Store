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
            System.out.println(message);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}