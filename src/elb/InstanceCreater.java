package elb;

import java.util.ArrayList;
import java.util.List;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CreateKeyPairRequest;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;

public class InstanceCreater {

    private BasicAWSCredentials bawsc;
    public static final Integer MIN_PROVISION_TIME = 90000;

    public InstanceCreater() {
        bawsc = Credential.getCredentials();
    }

    public String createInstance(String ami, String instance_type)
            throws InterruptedException {

        // Create an Amazon EC2 Client
        AmazonEC2Client ec2 = new AmazonEC2Client(bawsc);
        // Create Instance Request
        RunInstancesRequest runInstancesRequest = new RunInstancesRequest();

        CreateKeyPairRequest createKeyPairRequest = new CreateKeyPairRequest(
                "aws");
        createKeyPairRequest.setRequestCredentials(bawsc);

        runInstancesRequest.withImageId(ami).withInstanceType(instance_type)
                .withMinCount(1).withMaxCount(1).withMonitoring(true)
                .withKeyName(createKeyPairRequest.getKeyName());


        // Launch Instance
        RunInstancesResult runInstancesResult = ec2
                .runInstances(runInstancesRequest);
        // Return the Object Reference of the Instance just Launched

        Instance instance = runInstancesResult.getReservation().getInstances()
                .get(0);
        System.out.println("Running instance...");
        Thread.sleep(MIN_PROVISION_TIME);
        ec2.createTags(Tagger.getTagRequest(instance.getInstanceId()));

        return publicDNS(instance.getInstanceId());
    }

    public List<Instance> getInstances() {
        // Obtain a list of Reservations
        AmazonEC2Client amazonEC2Client = new AmazonEC2Client(bawsc);
        List<Reservation> reservations = amazonEC2Client.describeInstances()
                .getReservations();
        List<Instance> list = new ArrayList<Instance>();
        int reservationCount = reservations.size();
        for (int i = 0; i < reservationCount; i++) {
            List<Instance> instances = reservations.get(i).getInstances();
            int instanceCount = instances.size();
            //Print the instance IDs of every instance in the reservation.
            for (int j = 0; j < instanceCount; j++) {
                Instance instance = instances.get(j);
                if (instance.getState().getName().equals("running")) {
                    System.out.println(instance.getInstanceId());
                    list.add(instances.get(j));
                }
            }
        }
        return list;
    }

    public String publicDNS(String id) {
        AmazonEC2Client amazonEC2Client = new AmazonEC2Client(bawsc);
        List<Reservation> reservations = amazonEC2Client.describeInstances()
                .getReservations();
        int reservationCount = reservations.size();
        for (int i = 0; i < reservationCount; i++) {
            List<Instance> instances = reservations.get(i).getInstances();
            int instanceCount = instances.size();
            //Print the instance IDs of every instance in the reservation.
            for (int j = 0; j < instanceCount; j++) {
                Instance instance = instances.get(j);
                if (instance.getState().getName().equals("running")) {
                    if (instance.getInstanceId().equalsIgnoreCase(id)) {
                        return instance.getPublicDnsName();
                    }
                }
            }
        }
        return null;
    }

}