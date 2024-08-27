package distributed_system_project.message.body_parsers;
import distributed_system_project.utilities.Pair;

public class PutMessageBodyParser extends MessageBodyParser <Pair<String, String>> {

    public PutMessageBodyParser(String message_body) {
        super(message_body);
    }

    @Override
    public Pair<String, String> parse() {
        String[] split_message_body = this.message_body.trim().split("\\n");

        String key = split_message_body[0].trim();
        String value = split_message_body[1].trim();

        return new Pair<>(key, value);
    }
}
