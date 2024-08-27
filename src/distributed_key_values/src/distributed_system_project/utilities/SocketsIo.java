package distributed_system_project.utilities;
import java.io.*;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.Socket;

import distributed_system_project.message.Message;

public class SocketsIo {

    public static String readFromSocket(Socket socket){

        //TODO: correct the reader in App class
        try {
            // TODO: socket.setSoTimeout(100000);
            BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            String line;
            StringBuilder stringBuilder = new StringBuilder();

            while ((line = bufferedReader.readLine()) != null) {
                stringBuilder.append(line).append("\n");
                if(line.equals("end")){
                    break;
                }
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

    public static void sendUdpMessage(Message message, DatagramSocket socket,InetAddress ip_adress, int port ) throws IOException{

        byte[] buf = message.toString().getBytes();
        DatagramPacket packet = new DatagramPacket(buf, buf.length, ip_adress, port);

        socket.send(packet);
    }

}


