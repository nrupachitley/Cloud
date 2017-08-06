package elb.horizontal_scaling;

import elb.InstanceCreater;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;


public class Scaling {

    private static final String MEDIUM_INSTANCE_TYPE = "m3.medium";
    private static final String DATA_CENTER_INSTANCE_AMI = "ami-abb8cace";
    private static final String LOAD_GEN_INSTANCE_AMI = "ami-4389fb26";
    private static final String SUB_PASS = "mq2b0uKVflM57eRVjB2x9kmUtfoZ9Zlg";
    private static final Integer MIN_WAIT_TIME = 100000;

    private String load_generator_DNS = "";
    private boolean isPassSubmitted;
    private Double total_rps;
    private String test;

    public Scaling() {
        // rps = new HashMap<>();
        isPassSubmitted = false;
        total_rps = (double) 0;
    }

    /**
     * @param args
     * @throws IOException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws IOException,
            InterruptedException {
        // TODO Auto-generated method stub
        new Scaling().start_horizontal_scaling();
    }

    /**
     * @throws IOException
     * @throws InterruptedException
     */
    public void addDataCenter() throws IOException, InterruptedException {

        URL url = null;

        String newdata_center_DNS = new InstanceCreater().createInstance(
                DATA_CENTER_INSTANCE_AMI, MEDIUM_INSTANCE_TYPE);
        System.out.println("new data center" + newdata_center_DNS);
        String x = "http://" + load_generator_DNS + "/test/horizontal/add?dns="
                + newdata_center_DNS;
        System.out.println("x=" + x);
        url = new URL(x);

        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        if (connection.getResponseCode() == 200) {
            System.out.println("another data center added");
        } else {
            System.out.println(connection.getResponseMessage());
            System.out.println("Failed to add DC");
        }

    }

    /**
     * @return true of password is sucessfully submitted
     * @throws IOException
     */
    public boolean submitPassword() throws IOException {
        URL url = new URL("http://" + load_generator_DNS + "/password?passwd="
                + SUB_PASS);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        if (connection.getResponseCode() == 200) {
            System.out.println("password submitted");
            isPassSubmitted = true;
            return true;

        } else {
            System.out.println("Failure in password submitting");
            isPassSubmitted = false;
            return false;
        }
    }

    /**
     * @param newInstance the DNS of first new instance that is provisioned
     * @return true if test is sucessfully started
     * @throws IOException
     */
    public boolean start_test(String newInstance_DNS) throws IOException {
        System.out.println("NEW DNS=" + newInstance_DNS);
        URL url = new URL("http://" + load_generator_DNS
                + "/test/horizontal?dns=" + newInstance_DNS);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        if (connection.getResponseCode() == 200) {
            System.out.println("Test start Sucess");
            BufferedReader bufferedReader = new BufferedReader(
                    new InputStreamReader(connection.getInputStream()));
            String line = "";
            String[] tokens = null;
            while ((line = bufferedReader.readLine()) != null) {
                System.out.println(line);
                tokens = line.split("log?name=test.");
                tokens[0] = tokens[0].substring(tokens[0].indexOf("'") + 1);
                tokens[0] = tokens[0].substring(0, tokens[0].indexOf("'"));
            }
            test = tokens[0];
            System.out.println(test);
            return true;
        } else {
            System.out.println(connection.getResponseMessage());
            System.out.println("Test start failed");
            return false;

        }
    }

    /**
     * @param test_string to find the rps
     * @return the rps
     * @throws IOException
     */
    public double getCurrentRPS(String test_string) throws IOException {
        URL url = new URL("http://" + load_generator_DNS + test_string);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        double cumulative_rps = 0;
        if (connection.getResponseCode() == 200) {
            BufferedReader bufferedReader = new BufferedReader(
                    new InputStreamReader(connection.getInputStream()));
            String line = "";
            while ((line = bufferedReader.readLine()) != null) {
                if (!line.equals("")) {
                    if (line.contains("amazonaws.com=")) {
                        String[] tokens = line.split("=");
                        cumulative_rps += Double.parseDouble(tokens[1]);
                    } else {
                        if (line.contains("Minute"))
                            cumulative_rps = 0;
                    }
                }

            }
        } else {
            System.out.println("What the..");
        }
        return cumulative_rps;
    }

    /**
     * @throws IOException
     * @throws InterruptedException
     */
    public void start_horizontal_scaling() throws IOException,
            InterruptedException {
        InstanceCreater creater = new InstanceCreater();
        System.out.println("Provisioning Load generator");
        /**
         * create a load generator
         */
        load_generator_DNS = creater.createInstance(LOAD_GEN_INSTANCE_AMI,
                MEDIUM_INSTANCE_TYPE);
        System.out.println("LOAD=" + load_generator_DNS);
        /**
         * get first Instance. All your instances must be launched by your code.
         * All data center instances (except for the first one) should be
         * launched after the test starts.
         */
        System.out.println("Waiting to Provision...data denter");
        Thread.sleep(MIN_WAIT_TIME);
        System.out.println("Provisioning first Data center");
        String first_DNS = creater.createInstance(DATA_CENTER_INSTANCE_AMI,
                MEDIUM_INSTANCE_TYPE);
        System.out.println("DNS=" + first_DNS);
        /**
         * submit the password
         */
        if (isPassSubmitted == false) {
            submitPassword();
        }
        /**
         * start test
         */
        System.out.println("Beginning test");
        start_test(first_DNS);
        Thread.sleep(MIN_WAIT_TIME);
        total_rps = getCurrentRPS(test);

        try {
            while (total_rps <= 4000) {
                addDataCenter();
                total_rps = getCurrentRPS(test);
                System.out.println("RPS=" + total_rps);
                Thread.sleep(MIN_WAIT_TIME);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}