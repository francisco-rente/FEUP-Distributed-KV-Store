import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

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
    
}
