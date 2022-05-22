package distributed_system_project.body_parsers;

import distributed_system_project.body_parsers.message.Message;

import java.io.OutputStream;
import java.net.Socket;
import java.io.PrintWriter;
import java.util.Scanner;

// import FileUtils
import java.io.File;

public class App {
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
            String filePath = args[3];
            // read file content using Scanner
            Scanner scanner = new Scanner(new File(filePath));
            while (scanner.hasNextLine()) {
                bodyString.append(scanner.nextLine());
            }
            scanner.close();
        }

        if (operation.equals("delete") || operation.equals("get")) {
            // get key from args[3]3
            final String key = args[3];
            bodyString = new StringBuilder(key);
        }

        Message message = new Message(operation, true, nodeIp, nodePort, bodyString.toString());

        System.out.println("Creating Socket");
        Socket socket = new Socket(nodeIp, nodePort);

        System.out.println("Creating OutputStream");
        OutputStream output = socket.getOutputStream();
        PrintWriter writer = new PrintWriter(output, true);
        writer.println(message);

        /*
         * InputStream input = socket.getInputStream();
         * byte[] buffer = new byte[1024];
         * int bytesRead = input.read(buffer);
         * String response = new String(buffer, 0, bytesRead);
         * System.out.println(response);
         * socket.close();
         */

        // send and wait for the response

        // send socket message

    }
}



