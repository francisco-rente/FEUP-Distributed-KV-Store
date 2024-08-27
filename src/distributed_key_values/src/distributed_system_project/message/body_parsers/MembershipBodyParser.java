package distributed_system_project.message.body_parsers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import distributed_system_project.utilities.Pair;

public class MembershipBodyParser extends MessageBodyParser<Pair<List<String>, List<String>>> {

    public MembershipBodyParser(String message_body) {
        super(message_body);

    }

    /**
     * @return a pair in which the first element is the cluster list and the second list is the last32logs
     * if message does not contain cluster list, first list is empty
     */
    @Override
    public Pair<List<String>, List<String>> parse() {
        String [] clusterArr = {};
        String [] last32LogsArr;
        
        String[] split_message_body = this.message_body.trim().split("Logs:");

        String[] clusterInput = split_message_body[0].trim().split("List:");
        if(clusterInput.length !=1){
            String clusterString = clusterInput[1].trim();
            clusterArr = clusterString.split("\n");
        }
        String last32LogsString= split_message_body[1].trim();
        last32LogsArr = last32LogsString.split("\n");

        List<String> cluster = new ArrayList<String>();
        List<String> last32Logs = new ArrayList<String>();
        
        Collections.addAll(cluster, clusterArr);
        Collections.addAll(last32Logs, last32LogsArr); 


        return new Pair<>(cluster, last32Logs);
    }
    
}
