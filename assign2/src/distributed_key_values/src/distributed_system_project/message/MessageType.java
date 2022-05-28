package distributed_system_project.message;

import distributed_system_project.Store;

// enum for different message types
public enum MessageType {
    JOIN,
    LEAVE,
    GET,
    PUT,
    DELETE, 
    UNKNOWN;



    // convert enum type to string

    public static MessageType getMessageType(Message message, Store store) {
        return switch (message.getOperation()) {
            case "join" -> MessageType.JOIN;
            case "leave" -> MessageType.LEAVE;
            case "get" -> MessageType.GET;
            case "put" -> MessageType.PUT;
            case "delete" -> MessageType.DELETE;
            default -> MessageType.UNKNOWN;
        };
    }

}