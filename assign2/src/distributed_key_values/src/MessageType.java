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

        switch(message.getOperation()){
            case "join":
                return MessageType.JOIN;
            case "leave":
                return MessageType.LEAVE;
            case "get":
                return MessageType.GET;
            case "put":
                return MessageType.PUT;
            case "delete":
                return MessageType.DELETE;
            default:
                return MessageType.UNKNOWN;
        }
    }




}