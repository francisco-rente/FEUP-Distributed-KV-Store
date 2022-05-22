package distributed_system_project.body_parsers.message.body_parsers;
import distributed_system_project.body_parsers.utilities.Pair;

public class PutMessageBodyParser extends MessageBodyParser<Pair<String, String>> {

    public PutMessageBodyParser(String message_body) {
        super(message_body);
    }

    @Override
    public Pair<String, String> parse() {
        String[] split_message_body = this.message_body.split("\\n");

        String key = split_message_body[0];
        String value = split_message_body[1];
        return new Pair<>(key, value);
    }
}
