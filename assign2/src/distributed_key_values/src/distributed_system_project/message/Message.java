package distributed_system_project.message;

public class Message{

    private final String operation;
    private final boolean isTestClient;
    private final String ip;
    private final int port;
    private final String body;
    


    public boolean isTestClient() {
        return isTestClient;
    }

    public String getOperation() {
        return operation;
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    public String getBody() {
        return body;
    }

    public Message(String operation, boolean isTestClient, String ip, int port, String body){

        this.operation = operation;
        this.isTestClient = isTestClient;
        this.ip = ip;
        this.port = port;
        this.body = body;
    }

    public Message(){
        this.operation = "";
        this.isTestClient = false;
        this.ip = "";
        this.port = 0;
        this.body = null;
    }

    @Override
    public String toString(){
        String header = this.operation + " " + this.isTestClient + " " + this.ip + ":" + this.port + "\n\n"; 
        
        String body = "body:\n" + this.body;

        return header + body + "\nend";
    }

    public static Message toObject(String message){
        //create a Message object from the string
        String[] messageSplit = message.split("body:");
        String header = messageSplit[0];
        String padded_body = messageSplit[1];
        String body = padded_body.split("\nend")[0];

        // split header trimming newline characters
        String[] headerSplit = header.split(" ");

        String operation = headerSplit[0];

        String isTestClient = headerSplit[1];
        String ipPort = headerSplit[2];
        String[] ipPortSplit = ipPort.split(":");
        String ip = ipPortSplit[0];
        int port = Integer.parseInt(ipPortSplit[1].trim());
        // create a Message object
        return new Message(operation, Boolean.parseBoolean(isTestClient), ip, port, body);
    }

}