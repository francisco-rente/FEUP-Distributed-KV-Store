package distributed_system_project;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class FileSystem {

    public static boolean writeOnFile(String path, String text) {
        FileWriter myWriter;
        try {
            myWriter = new FileWriter(path, true);
            BufferedWriter bWriter = new BufferedWriter(myWriter);
            bWriter.write(text);
            bWriter.newLine();
            bWriter.close();
    
        } catch (IOException e) {
            // TODO Auto-generated catch block
            return false;
        }

        return true;
        
    }

    public static boolean writeTextOnFile(String path ,List<String> last32Logs){

        FileWriter myWriter;
        try {
            myWriter = new FileWriter(path, false);
            BufferedWriter bWriter = new BufferedWriter(myWriter);

            for(String log: last32Logs){
                bWriter.write(log);
                bWriter.newLine();
            }
            
            bWriter.close();
    
        } catch (IOException e) {
            // TODO Auto-generated catch block
            return false;
        }

        return true;
    }

    /**
     * Receieves a log and takes from it the store ip and the membership counter information
     * @param log 
     * @return A list of Strings whic first is the store IP and second is the membership counter
     */
    public static ArrayList<String> logToInfo(String log){
        String[] logInfo = log.split(" ");

        ArrayList<String> info = new ArrayList<String>();

        info.add(logInfo[0]);
        info.add(logInfo[1]);

        return info;

    }

    /**
     * Returns the log from the store information array
     * @param info The array must have the store ip on position 0 and its memebership counter on the next
     * @return A string containing the log
     */
    public static String infoToLog(ArrayList<String> info){
        return info.get(0) + " " + info.get(1);
    }
    
}
