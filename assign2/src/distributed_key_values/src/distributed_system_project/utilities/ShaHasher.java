package distributed_system_project.utilities;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;


public abstract class ShaHasher {

    public ShaHasher() {
    }


    public static String getHashString(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(input.getBytes());
            byte[] digest = md.digest();
            StringBuilder sb = new StringBuilder();
            for (byte b : digest) {
                sb.append(String.format("%02x", b & 0xff));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return "";
    }

    public static byte[] getHashBytes(String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(input.getBytes());
            return md.digest();
        } catch (NoSuchAlgorithmException e) {
            e.printStackTrace();
        }
        return null;
    }


}
