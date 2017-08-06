package elb;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;
import com.amazonaws.auth.BasicAWSCredentials;

/**
 *
 * @author Nrupa
 *
 */
public class Credential {
    private static Credential instance;
    private static final String KEY_FILE = "rootkey.properties";
    private static Properties properties;


    /**
     *
     * @return
     */
    public Properties getProperties() {
        return properties;
    }

    /**
     *
     * @param filename
     */
    public void setProperties(String filename) {
        try {
            properties.load(new FileReader(new File(KEY_FILE)));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * @return the single instance of Credentials after properties is set
     */
    public static BasicAWSCredentials getCredentials() {
        if (instance == null) {
            properties = new Properties();
            try {
                properties.load(new FileReader(new File(KEY_FILE)));
                BasicAWSCredentials bawsc=new BasicAWSCredentials(properties.getProperty("AWSAccessKeyId"),
                        properties.getProperty("AWSSecretKey"));
                return bawsc;
            } catch (IOException e) {
                e.printStackTrace();
            }
            instance = new Credential();
        }
        return null;
    }
}