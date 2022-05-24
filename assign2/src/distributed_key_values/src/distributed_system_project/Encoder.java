package distributed_system_project;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Encoder {
    
    private static String bytesToHex(byte[] hash) {
        StringBuilder hexString = new StringBuilder(2 * hash.length);
        for (int i = 0; i < hash.length; i++) {
            String hex = Integer.toHexString(0xff & hash[i]);
            if(hex.length() == 1) {
                hexString.append('0');
            }
            hexString.append(hex);
        }
        return hexString.toString();
    }



    public static String encryptSHA(String value) throws NoSuchAlgorithmException, UnsupportedEncodingException {
        MessageDigest digest;
        
        digest = MessageDigest.getInstance("SHA3-256");
        final byte[] hashbytes = digest.digest(
        value.getBytes("utf-8"));
        return bytesToHex(hashbytes);
    }

}
