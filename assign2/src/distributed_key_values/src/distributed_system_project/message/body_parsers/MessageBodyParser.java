package distributed_system_project.message.body_parsers;

public abstract class MessageBodyParser <T>{

    final String message_body;

    public MessageBodyParser(String message_body) {
        this.message_body = message_body;
    }

    // parse the message body and return the parsed object
    public abstract T parse();

    public String get_message_string(){
        return this.message_body;
    }

    public String parseResponseToRequest() {return this.message_body.trim(); }

}
