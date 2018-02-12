#seperate control
#e.x, in order to build jar file, : make -f make.mk build 
project="/Users/yzh/IdeaProjects/mapreduce get time series by secondary sort"
jar="/Users/yzh/IdeaProjects/mapreduce get time series by secondary sort/target/wc-1.0.jar"

hadoop="/usr/local/hadoop-2.7.5"
input="/Users/yzh/Desktop/njtest/input"
output="/Users/yzh/Desktop/njtest/output"








awsoutput="s3://michaelyangcs/output2ndsortSmall"
localout="/Users/yzh/Desktop/cour/parallel/output2ndsortSmall"

.PHONY:build
build:
	cd ${project};  mvn clean install;

.PHONY:standalone
standalone: 
	cd ${hadoop}; rm -rf ${output}; bin/hadoop jar ${jar} sort2nd ${input} ${output} "";


.PHONY:awsrun
awsrun:
    
#	aws s3 rm ${awsoutput} --recursive
	aws emr create-cluster \
    --name "secondsortSmall" \
    --release-label emr-5.11.1 \
    --instance-groups '[{"InstanceCount":5,"InstanceGroupType":"CORE","InstanceType":"m4.large"},{"InstanceCount":1,"InstanceGroupType":"MASTER","InstanceType":"m4.large"}]' \
    --applications Name=Hadoop \
    --steps '[{"Args":["sort2nd","s3://michaelyangcs/input_small2","s3://michaelyangcs/output2ndsortSmall",""],"Type":"CUSTOM_JAR","Jar":"s3://michaelyangcs/wc-1.0.jar","ActionOnFailure":"TERMINATE_CLUSTER","Name":"Custom JAR"}]' \
--log-uri s3://michaelyangcs/log2ndsortSmall \
--service-role EMR_DefaultRole \
--ec2-attributes InstanceProfile=EMR_EC2_DefaultRole,SubnetId=subnet-520b7f0f \
--region us-east-1 \
--enable-debugging \
--auto-terminate

.PHONY:sync
sync:
	aws s3 sync ${awsoutput} ${localout}
	



