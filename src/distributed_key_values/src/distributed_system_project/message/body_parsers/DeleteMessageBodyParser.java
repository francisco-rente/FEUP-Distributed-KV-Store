package distributed_system_project.message.body_parsers;

public class DeleteMessageBodyParser extends MessageBodyParser<String> {

    public DeleteMessageBodyParser(String message_body) {
        super(message_body);
    }

    @Override
    public String parse() {
        return this.message_body.trim();
    }
}
