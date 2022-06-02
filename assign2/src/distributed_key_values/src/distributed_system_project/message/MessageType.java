package distributed_system_project.message;

import distributed_system_project.Store;

// enum for different message types
public enum MessageType {
    JOIN,
    LEAVE,
    GET,
    PUT,
    DELETE,
    MEMBERSHIP, 
    UNKNOWN;



    // convert enum type to string

    public static MessageType getMessageType(Message message, Store store) {

        switch (message.getOperation()){
            case "join":
                return JOIN;
            case "leave":
                return LEAVE;
            case "get":
                return GET;
            case "put":
                return PUT;
            case "delete":
                return DELETE;
            case "membership":
                return MEMBERSHIP;
            default:
                return UNKNOWN;
        }
    }

}