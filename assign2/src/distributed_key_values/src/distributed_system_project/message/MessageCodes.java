package distributed_system_project.message;

public abstract class MessageCodes {

    public static final String FILE_NOT_FOUND = "File not found.";
    public static final String ERROR_CONNECTING = "Error connecting to store";
    public static final String PUT_SUCCESS = "Put successful";
    public static final String PUT_FAIL = "Put failed";
    public static final String GET_FAIL = "Get failed";
    public static final String DELETE_SUCCESS = "Delete successful";
    public static final String DELETE_FAIL = "Delete failed";
    public static final String FILE_EXISTS = "File already exists";
    public static final String ERROR_SAVING_FILE = "Error saving file";
    public static final String UPDATED_WITH_JOIN = "New node joined";
    public static final String ERROR_READING_FILE = "Error reading file";
    public static final String UPDATE_CLUSTER_SUCCESS = "UPDATED CLUSTER";
    public static final String UPDATE_CLUSTER_FAIL = "FAILED TO UPDATE CLUSTER";

    MessageCodes() {
    }

}