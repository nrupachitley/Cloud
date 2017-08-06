/**
 *
 */
package elb.auto_scaling;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.AuthorizeSecurityGroupIngressRequest;
import com.amazonaws.services.ec2.model.CreateSecurityGroupRequest;
import com.amazonaws.services.ec2.model.IpPermission;

/**
 * @author Nrupa
 */
public class CreateSecurityGroup {

    /**
     * @return
     */
    private List<IpPermission> setIPpermission() {
        String ipAddr = "0.0.0.0/0";
        List<String> ipRanges = Collections.singletonList(ipAddr);
        IpPermission ipPermission = new IpPermission().withIpProtocol("-1")
                .withFromPort(new Integer(0)).withToPort(new Integer(65535))
                .withIpRanges(ipRanges);

        List<IpPermission> ipPermissions = Collections.singletonList(ipPermission);
        return ipPermissions;

    }

    /**
     * create a security group with all traffic open for all ports.
     *
     * @return ingress request which can be passed to ec2 instance
     */

    public String securityGorup_all_open(String resource) {
        System.out.println("Creating Secutiry Gorup for " + resource);
        AuthorizeSecurityGroupIngressRequest ingressRequest = new AuthorizeSecurityGroupIngressRequest();
        ingressRequest.withIpPermissions((Collection<IpPermission>) setIPpermission())
                .withGroupName(resource);
        CreateSecurityGroupRequest createSecurityGroupRequest = new CreateSecurityGroupRequest();
        createSecurityGroupRequest.withGroupName(resource)
                .withDescription("SG with all traffic open")
                .withVpcId("vpc-c67280a2");
        AmazonEC2Client amazonEC2Client = new AmazonEC2Client();
        amazonEC2Client.createSecurityGroup(createSecurityGroupRequest);
        amazonEC2Client.authorizeSecurityGroupIngress(ingressRequest);
        /**
         * creating SG takes time
         */
        try {
            Thread.sleep(60000);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return ingressRequest.getGroupId();
    }
}