package elb.auto_scaling;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.RegionUtils;
import com.amazonaws.services.autoscaling.AmazonAutoScalingClient;
import com.amazonaws.services.autoscaling.model.CreateAutoScalingGroupRequest;
import com.amazonaws.services.autoscaling.model.CreateOrUpdateTagsRequest;
import com.amazonaws.services.autoscaling.model.PutScalingPolicyRequest;
import com.amazonaws.services.autoscaling.model.PutScalingPolicyResult;
import com.amazonaws.services.autoscaling.model.Tag;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchClient;
import com.amazonaws.services.cloudwatch.model.ComparisonOperator;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.PutMetricAlarmRequest;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.amazonaws.services.cloudwatch.model.Statistic;

/**
 *
 * @author Nrupa
 *
 */
public class AutoScalingGroupCreater {

    public static final Integer MIN_ASG_INSTNACE = 1;
    public static final Integer MAX_ASG_INSTNACE = 4;

    public static final Integer UP_Scaling_Adjustment=1;
    public static final Integer DOWN_Scaling_Adjustment=-1;
    public static final String SCALE_UP="ASGscaleup";
    public static final String SCALE_DOWN="ASGscaledown";
    public static final String 	Adjustment_Type="ChangeInCapacity";
    //Cloud Watch Alarm Policies
    public static final Integer EVALUATION_PERIOD=1;
    public static final Integer PERIOD=60;
    public static final Double SCALE_UP_CPU_THRESHOLD=70.0;
    public static final Double SCALE_DOWN_CPU_THRESHOLD=30.0;

    private AmazonAutoScalingClient amazonAutoScalingClient;
    private AmazonCloudWatchClient amazonCloudWatchClient;

    public AutoScalingGroupCreater(){
        BasicAWSCredentials credentials=Credential.getCredentials();
        Region region=RegionUtils.getRegion(Statics.Region);

        amazonAutoScalingClient=new AmazonAutoScalingClient(credentials);
        amazonAutoScalingClient.withRegion(region);
        amazonCloudWatchClient=new AmazonCloudWatchClient(credentials);
        amazonCloudWatchClient.withRegion(region);
    }

    /**
     * @throws InterruptedException
     *
     */

    public void createAutoScalingGroup() throws InterruptedException{

        CreateAutoScalingGroupRequest autoScalingGroupRequest=new CreateAutoScalingGroupRequest();
        autoScalingGroupRequest
                .withAutoScalingGroupName(Statics.ASG_NAME)
                .withAvailabilityZones(Statics.Availability_zone)
                .withLaunchConfigurationName(Statics.Launch_Configuration_Name)
                .withLoadBalancerNames(Statics.LB_NAME)
                .withMaxSize(MAX_ASG_INSTNACE)
                .withMinSize(MIN_ASG_INSTNACE)
                .withHealthCheckGracePeriod(60)
                .withHealthCheckType("ELB")
                .withDefaultCooldown(300)

        ;
        setLaunchConfiguration();
        System.out.println("ASG is being created..check AWS");
        amazonAutoScalingClient.createAutoScalingGroup(autoScalingGroupRequest);
        cloudWatchUpAlarm(scale_up_policy());
        cloudWatchDownAlarm(scale_down_policy());

        amazonAutoScalingClient.createOrUpdateTags(new CreateOrUpdateTagsRequest().withTags(getTag()));

    }

    private Tag[] getTag() {
        // TODO Auto-generated method stub
        Tag tag=new Tag();

        tag.withKey("Project")
                .withPropagateAtLaunch(true)
                .withValue("2.2")
                .withResourceType("auto-scaling-group")
                .withResourceId(Statics.ASG_NAME);

        Tag[] tags=new Tag[1];
        tags[0]=tag;

        return tags;
    }

    /**
     *
     * @throws InterruptedException
     */
    public void setLaunchConfiguration() throws InterruptedException{
        amazonAutoScalingClient.createLaunchConfiguration(new LaunchConfigurationCreator().createLaunchConfiguration());
    }
    /**
     * called as an argument for cloudWatchUpAlarm.
     * @return
     */
    public String scale_up_policy(){
        System.out.println("Creating Up policy");
        PutScalingPolicyRequest policyRequest=new PutScalingPolicyRequest();
        policyRequest.withAutoScalingGroupName(Statics.ASG_NAME)
                .withAdjustmentType(Adjustment_Type)
                .withScalingAdjustment(UP_Scaling_Adjustment)
                .withPolicyName(SCALE_UP);
        PutScalingPolicyResult policyResult=amazonAutoScalingClient.putScalingPolicy(policyRequest);
        System.out.println(policyResult.getPolicyARN());
        return policyResult.getPolicyARN();
    }

    /**
     * called as an argument for cloudWatchDownAlarm.
     * @return
     */
    public String scale_down_policy(){
        System.out.println("Creating Down policy");
        PutScalingPolicyRequest policyRequest=new PutScalingPolicyRequest();
        policyRequest.withAutoScalingGroupName(Statics.ASG_NAME)
                .withAdjustmentType(Adjustment_Type)
                .withScalingAdjustment(DOWN_Scaling_Adjustment)
                .withPolicyName(SCALE_DOWN);
        PutScalingPolicyResult policyResult=amazonAutoScalingClient.putScalingPolicy(policyRequest);

        return policyResult.getPolicyARN();
    }
    /**
     *
     * @param arn
     */
    public void cloudWatchUpAlarm(String arn){
        System.out.println("Creating Cloud Watch Alarm for Up policy");
        PutMetricAlarmRequest alarmRequest=new PutMetricAlarmRequest();
        alarmRequest.withAlarmName("ScaleUpAlarm")
                .withComparisonOperator(ComparisonOperator.GreaterThanOrEqualToThreshold)
                .withEvaluationPeriods(EVALUATION_PERIOD)
                .withPeriod(PERIOD)
                .withStatistic(Statistic.Average)
                .withThreshold(SCALE_UP_CPU_THRESHOLD)
                .withUnit(StandardUnit.Percent)
                .withAlarmActions(arn)
                .withNamespace("AWS/EC2")
                .withMetricName("CPUUtilization")
                .withDimensions(new Dimension().withName("AutoScalingGroupName").withValue(Statics.ASG_NAME))
                .withActionsEnabled(true)
        ;
        amazonCloudWatchClient.putMetricAlarm(alarmRequest);
    }
    /**
     *
     * @param arn
     */
    public void cloudWatchDownAlarm(String arn){
        System.out.println("Creating Cloud Watch Alarm for Down policy");
        PutMetricAlarmRequest alarmRequest=new PutMetricAlarmRequest();
        alarmRequest.withAlarmName("ScaleDownAlarm")
                .withComparisonOperator(ComparisonOperator.LessThanOrEqualToThreshold)
                .withEvaluationPeriods(EVALUATION_PERIOD)
                .withPeriod(PERIOD)
                .withStatistic(Statistic.Average)
                .withThreshold(SCALE_DOWN_CPU_THRESHOLD)
                .withUnit(StandardUnit.Percent)
                .withAlarmActions(arn)
                .withNamespace("AWS/EC2")
                .withMetricName("CPUUtilization")
                .withDimensions(new Dimension().withName("AutoScalingGroupName").withValue(Statics.ASG_NAME))
                .withActionsEnabled(true)
        ;
        amazonCloudWatchClient.putMetricAlarm(alarmRequest);
    }


}