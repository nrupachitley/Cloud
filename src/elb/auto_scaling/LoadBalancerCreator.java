package elb.auto_scaling;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.elasticloadbalancing.AmazonElasticLoadBalancingClient;
import com.amazonaws.services.elasticloadbalancing.model.ApplySecurityGroupsToLoadBalancerRequest;
import com.amazonaws.services.elasticloadbalancing.model.ConfigureHealthCheckRequest;
import com.amazonaws.services.elasticloadbalancing.model.CreateLoadBalancerRequest;
import com.amazonaws.services.elasticloadbalancing.model.CreateLoadBalancerResult;
import com.amazonaws.services.elasticloadbalancing.model.HealthCheck;
import com.amazonaws.services.elasticloadbalancing.model.InvalidSecurityGroupException;
import com.amazonaws.services.elasticloadbalancing.model.Listener;
import com.amazonaws.services.elasticloadbalancing.model.Tag;
import elb.Credential;

/**
 * @author Nrupa
 *
 */
public class LoadBalancerCreator {
    public static final String ELB_PROTOCOL = "HTTP";
    public static final String INSTANCE_PROTOCOL = "HTTP";
    public static final Integer ELB_PORT = 80;
    public static final Integer INSTANCE_PORT = 80;
    public static final String ELB_PATH_TO_PING = "/heartbeat?lg=";
    public static final Integer Healthy_threshold = 10;
    public static final Integer Unhealthy_threshold = 7;
    public static final Integer Health_check_interval = 10;
    public static final Integer Response_timeout = 7;
    private BasicAWSCredentials bawsc;

    public LoadBalancerCreator() {
        bawsc = Credential.getCredentials();
    }

    private Tag getTag() {
        Tag tag = new Tag();
        tag.setKey("Project");
        tag.setValue("2.2");
        return tag;
    }

    private HealthCheck getHealthCheck(String LG_GEN_NAME) {
        HealthCheck check = new HealthCheck();
        check.withTarget(ELB_PROTOCOL + ":" + ELB_PORT + ELB_PATH_TO_PING+LG_GEN_NAME)
                .withHealthyThreshold(Healthy_threshold)
                .withInterval(Health_check_interval)
                .withTimeout(Response_timeout)
                .withUnhealthyThreshold(Unhealthy_threshold)

        ;

        return check;
    }

    private ApplySecurityGroupsToLoadBalancerRequest createSecurityGroup(){
        CreateSecurityGroup createSecurityGroup=new CreateSecurityGroup();
        String sg_id=createSecurityGroup.securityGorup_all_open(Statics.LB_NAME);
        ApplySecurityGroupsToLoadBalancerRequest applySecurityGroupsToLoadBalancerRequest=new ApplySecurityGroupsToLoadBalancerRequest();
        applySecurityGroupsToLoadBalancerRequest.withLoadBalancerName(Statics.LB_NAME)
                .withSecurityGroups(sg_id);

        return applySecurityGroupsToLoadBalancerRequest;
    }

    /**
     * @throws InterruptedException
     *
     */
    public String createLoadBalancer(String LG_DNS) throws Exception {
        AmazonElasticLoadBalancingClient amazonElasticLoadBalancingClient = new AmazonElasticLoadBalancingClient(
                bawsc);
        ConfigureHealthCheckRequest checkRequest=new ConfigureHealthCheckRequest();
        checkRequest.withHealthCheck(getHealthCheck(LG_DNS))
                .withLoadBalancerName(Statics.LB_NAME)
        ;


        CreateLoadBalancerRequest balancerRequest = new CreateLoadBalancerRequest();

        /**
         * add listener
         */
        Listener listener = new Listener();
        listener.withProtocol(ELB_PROTOCOL).withInstancePort(ELB_PORT)
                .withInstanceProtocol(INSTANCE_PROTOCOL)
                .withInstancePort(INSTANCE_PORT).withLoadBalancerPort(ELB_PORT);
        ApplySecurityGroupsToLoadBalancerRequest applySecurityGroupsToLoadBalancerRequest=null;
        try{
            applySecurityGroupsToLoadBalancerRequest=createSecurityGroup();
            balancerRequest.withLoadBalancerName(Statics.LB_NAME)
                    .withTags(getTag())
                    .withListeners(listener)
                    .withAvailabilityZones(Statics.Availability_zone)
            ;
        }
        catch(InvalidSecurityGroupException e){
            System.out.println("Cought");
            e.printStackTrace();
            return null;
        }
        CreateLoadBalancerResult balancerResult = amazonElasticLoadBalancingClient
                .createLoadBalancer(balancerRequest);
        amazonElasticLoadBalancingClient.configureHealthCheck(checkRequest);
        amazonElasticLoadBalancingClient.applySecurityGroupsToLoadBalancer(applySecurityGroupsToLoadBalancerRequest);


        System.out.println("ELB is being creatd");
        Thread.sleep(Statics.MIN_PROVISION_TIME);
        return balancerResult.getDNSName();

    }

}