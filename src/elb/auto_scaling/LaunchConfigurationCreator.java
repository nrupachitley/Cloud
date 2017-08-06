package elb.auto_scaling;
import com.amazonaws.services.autoscaling.model.BlockDeviceMapping;
import com.amazonaws.services.autoscaling.model.CreateLaunchConfigurationRequest;
import com.amazonaws.services.autoscaling.model.Ebs;
import com.amazonaws.services.autoscaling.model.InstanceMonitoring;

public class LaunchConfigurationCreator {

    public CreateLaunchConfigurationRequest createLaunchConfiguration() throws InterruptedException{
        CreateLaunchConfigurationRequest configurationRequest=new CreateLaunchConfigurationRequest();

        /**
         * creating DCs with 30GB
         */

        configurationRequest.withImageId(Statics.DATA_CENTER_INSTANCE_AMI)
                .withLaunchConfigurationName(Statics.Launch_Configuration_Name)
                .withInstanceType(Statics.MEDIUM_INSTANCE_TYPE)
                .withSecurityGroups(new CreateSecurityGroup().securityGorup_all_open(Statics.LC_SG_NAME))
        ;
        InstanceMonitoring instanceMonitoring=new InstanceMonitoring();
        instanceMonitoring.setEnabled(true);
        configurationRequest.withInstanceMonitoring(instanceMonitoring)

        ;
        System.out.println("Launch Configuratoin is getting created...");

        //Thread.sleep(Statics.MIN_WAIT_TIME);
        return configurationRequest;
    }
}