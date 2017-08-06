package elb.auto_scaling;

public class Statics {
    //Others
    public static final String MEDIUM_INSTANCE_TYPE = "m3.medium";
    public static final String MICRO_INSTANCE_TYPE = "t2.micro";
    public static final String DATA_CENTER_INSTANCE_AMI = "ami-3b2b515e";
    public static final String UBUNTU_AMI="ami-d05e75b8";
    public static final String LOAD_GEN_INSTANCE_AMI = "ami-312b5154";
    public static final String SUB_PASS = "mq2b0uKVflM57eRVjB2x9kmUtfoZ9Zlg";
    public static final String LB_NAME="ELB";
    public final static String ASG_NAME="MSBasg";
    public final static String ELB_SG_NAME="sg-elb";
    public final static String LC_SG_NAME="lc";
    public static final Integer MIN_WAIT_TIME = 100000;
    public static final Integer WARMUP_WAIT_TIME =300000;
    public static final String Availability_zone = "us-east-1b";
    public static final String Region = "us-east-1";
    public static final String Launch_Configuration_Name="MSBlc";
    public static final String BURST_TEST="burst";
    public static final String PLATAUE_TEST="plat";
    public static final String JUNIOR_TEST="junior";
    //Scaling Rules
    public static final Integer MIN_PROVISION_TIME = 100000;
}