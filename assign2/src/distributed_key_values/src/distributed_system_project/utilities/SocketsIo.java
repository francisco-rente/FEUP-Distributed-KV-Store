package distributed_system_project.utilities;
import java.io.*;
import java.net.Socket;

public class SocketsIo {

    public static String readFromSocket(Socket socket){

        //TODO: correct the reader in App class
        try {
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String line;
            StringBuilder stringBuilder = new StringBuilder();
            while (bufferedReader.ready() && (line = bufferedReader.readLine()) != null) {
                stringBuilder.append(line).append("\n");
            }
            return stringBuilder.toString();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void sendStringToSocket(String response, Socket socket) {
        OutputStream outputStream;
        try {
            outputStream = socket.getOutputStream();
            PrintWriter printWriter = new PrintWriter(outputStream, true);
            printWriter.println(response);
            printWriter.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}


