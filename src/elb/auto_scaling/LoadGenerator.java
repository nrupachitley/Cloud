package elb.auto_scaling;

import elb.InstanceCreater;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

public class LoadGenerator extends InstanceCreater {
    private static boolean isPassSubmitted = false;

    /**
     *
     * @param ami
     * @param instance_type
     * @return
     * @throws InterruptedException
     */
    public String createLoadgenerator(String ami, String instance_type)
            throws InterruptedException {
        String load_gen_DNS = createInstance(ami, instance_type);
        return load_gen_DNS;
    }

    /**
     *
     * @param elb_dns
     * @param test
     * @return
     * @throws java.io.IOException
     */

    public boolean startTest(String load_gen_dns, String elb_dns, String test)
            throws IOException {

        String testurl = "";
        switch (test) {
            case Statics.BURST_TEST: {
                testurl = "http://" + load_gen_dns + "/burst?dns=" + elb_dns;
                break;
            }
            case Statics.PLATAUE_TEST: {
                testurl = "http://" + load_gen_dns + "/plat?dns=" + elb_dns;
                break;
            }
            case Statics.JUNIOR_TEST: {
                testurl = "http://" + load_gen_dns + "/junior?dns=" + elb_dns;
            }
        }
        URL url = new URL(testurl);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        BufferedReader br;
        if (connection.getResponseCode() == 200) {
            br = new BufferedReader(
                    new InputStreamReader(connection.getInputStream()));
            String line = "";
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
            return true;
        } else {
            System.err.println("Oops, i did it again"
                    + connection.getResponseMessage());
            br = new BufferedReader(
                    new InputStreamReader(connection.getErrorStream()));
            String line = "";
            while ((line = br.readLine()) != null) {
                System.out.println(line);
            }
        }
        return false;
    }

    /**
     *
     *
     * @param elb_dns
     * @return
     * @throws IOException
     */
    public boolean warmup(String load_gen_dns, String elb_dns)
            throws IOException {

        String testurl = "http://" + load_gen_dns + "/warmup?dns=" + elb_dns;
        System.out.println(testurl);
        URL url = new URL(testurl);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("GET");
        BufferedReader bufferedReader;
        String line = "";
        if (connection.getResponseCode() == 200) {
            bufferedReader = new BufferedReader(
                    new InputStreamReader(connection.getInputStream()));

            while ((line = bufferedReader.readLine()) != null) {
                System.out.println(line);
                return true;
            }
        } else {
            System.err.println("Oops, i did it again "
                    + connection.getResponseMessage());
            bufferedReader = new BufferedReader(
                    new InputStreamReader(connection.getErrorStream()));
            while ((line = bufferedReader.readLine()) != null) {
                System.out.println(line);
            }
        }
        return false;
    }

    /**
     *
     * @return true of password is sucessfully submitted
     * @throws IOException
     */
    public boolean submitPassword(String load_generator_DNS) throws IOException {
        URL url = new URL("http://" + load_generator_DNS + "/password?passwd="
                + Statics.SUB_PASS);
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

}