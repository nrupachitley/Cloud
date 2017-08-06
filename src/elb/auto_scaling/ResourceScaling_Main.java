package elb.auto_scaling;

public class ResourceScaling_Main {

    public static void main(String[] args) {

        try {
            Loadgenerator loadgenerator = new Loadgenerator();

            String LG_DNS = loadgenerator.createLoadgenerator(
                    Statics.LOAD_GEN_INSTANCE_AMI, Statics.MEDIUM_INSTANCE_TYPE);

            System.out.println("Creating Load Balnacer");
            String ELB_DNS = "";

            ELB_DNS = new LoadBalancerCreater().createLoadBalancer(LG_DNS);

            System.out.println("Load Balancer DNS= " + ELB_DNS);
            new AutoScalingGroupCreater().createAutoScalingGroup();
            Thread.sleep((int)(1.5*Statics.MIN_WAIT_TIME));

            loadgenerator.submitPassword(LG_DNS);
            loadgenerator.warmup(LG_DNS, ELB_DNS);

            Thread.sleep(Statics.WARMUP_WAIT_TIME);
            System.out.println("Starting Junior Test...");
            loadgenerator.startTest(LG_DNS, ELB_DNS, Statics.JUNIOR_TEST);

        } catch (Exception e) {
            // TODO Auto-generated catch block
            System.err.println("Somwthign went wrong");
            e.printStackTrace();
        }
    }

}