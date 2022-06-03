package distributed_system_project;

import distributed_system_project.message.Message;
import distributed_system_project.utilities.ShaHasher;
import distributed_system_project.utilities.SocketsIo;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Scanner;

// import FileUtils


public class TestClient {
    public static void main(String[] args) throws Exception {
        if (args.length < 2 || args.length > 4) {
            System.out.println(
                    "Error in number of arguments. Please write something like this on terminal:\n distributed_system_project.body_parsers.App nodeIp:nodePort operation [operator]");
            return;
        }

        // get node ip and port tuple from args[1]
        String[] nodeAddressSplit = args[0].split(":");
        String nodeIp = nodeAddressSplit[0];
        int nodePort = Integer.parseInt(nodeAddressSplit[1]);

        System.out.println("nodeIp: " + nodeIp + " nodePort: " + nodePort);

        // get operation from args[2]
        final String operation = args[1];
        if (!operation.equals("get") && !operation.equals("put") && !operation.equals("delete")
                && !operation.equals("join") && !operation.equals("leave")) {
            System.out.println(
                    "Error in operation. Please write something like this on terminal:\n distributed_system_project.body_parsers.App nodeIp:nodePort operation [operator]");
            return;
        }

        StringBuilder bodyString = new StringBuilder();

        if (operation.equals("put")) {
            // get sufix from file path in args[2]
            String filePath = args[2];

            if (!new File(filePath).exists()) throw new FileNotFoundException();
            byte[] encoded = Files.readAllBytes(Paths.get(filePath));
            String fileContent = new String(encoded, StandardCharsets.UTF_8);

            final String key = ShaHasher.getHashString(fileContent);
            System.out.println("fileKey: " + key);
            // read file content using Scanner
            bodyString.append(key).append("\n");
            Scanner scanner = new Scanner(new File(filePath));
            while (scanner.hasNextLine()) {
                bodyString.append(scanner.nextLine());
            }
            scanner.close();
        }

        if (operation.equals("delete") || operation.equals("get")) {
            // get key from args[3]3z
            final String key = args[2];
            System.out.println("key: " + key);
            bodyString = new StringBuilder(key);
        }

        Message message = new Message(operation, true, nodeIp, nodePort, bodyString.toString());

        System.out.println("Creating Socket to node: " + nodeIp + ":" + nodePort);
        System.out.println("Sending message:\n------------ \n" + message + "\n-----------------");
        Socket socket = new Socket(nodeIp, nodePort);

        OutputStream output = socket.getOutputStream();
        PrintWriter writer = new PrintWriter(output, true);
        writer.println(message);
        writer.flush();

        // wait for response
        System.out.println("Waiting for response");
        String response = SocketsIo.readFromSocket(socket);
        System.out.println("Response: " + response);
        socket.close();
    }
}



