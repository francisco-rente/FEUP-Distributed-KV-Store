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


    @Override
    public String toString() {
        switch (this) {
            case JOIN:
                return "join";
            case LEAVE:
                return "leave";
            case GET:
                return "get";
            case PUT:
                return "put";
            case DELETE:
                return "delete";
            default:
                return "unknown";
        }
    }

    // convert enum type to string

    public static MessageType getMessageType(Message message) {

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