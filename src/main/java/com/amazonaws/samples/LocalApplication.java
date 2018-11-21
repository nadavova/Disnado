package com.amazonaws.samples;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.IamInstanceProfileSpecification;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.StartInstancesRequest;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;
//import com.amazonaws.util.Base64;


public class LocalApplication {
	private static AWSCredentialsProvider credentialsProvider = new AWSStaticCredentialsProvider(new ProfileCredentialsProvider().getCredentials());
	protected static AmazonEC2 ec2;
	private static AmazonS3 s3;
	private static AmazonSQS sqs;
	private static String bucketName = credentialsProvider.getCredentials().getAWSAccessKeyId().toLowerCase();
	//private static String mySendQueueUrlName = "local_send_manager_queue";
	//private static String myReceiveQueueUrlName = "local_receive_manager_queue";
	private static String newTask = "new task";
	private static String doneTask = "done task";
	private static String mySendQueueUrl, myReceiveQueueUrl;
	private static Map<String,String> input_output_files;
	private static String sqsLocalManagerFileUpload = "sqsLocalManagerFileUpload";
	private static String sqsManagerLocalFileDone = "sqsManagerLocalFileDone";
	public static IamInstanceProfileSpecification instanceP;

	public static void main(String[] args) {
		long startTime = System.currentTimeMillis();
		boolean terminate = false;
		System.out.println("bucketName: " + bucketName);
		if(args[args.length - 1].equals("terminate")) {
			terminate = true;
		}

		//for storage
		s3 = AmazonS3ClientBuilder.standard()
				.withCredentials(credentialsProvider)
				.withRegion("us-east-1")
				.build();
		//queue
		sqs = AmazonSQSClientBuilder.standard()
				.withCredentials(credentialsProvider)
				.withRegion("us-east-1")
				.build();
		
		instanceP = new IamInstanceProfileSpecification();
		instanceP.setArn("arn:aws:iam::692054548727:instance-profile/EgorNadavRole");
		createS3();
		//uploadFiles(s3 , args);
		mySendQueueUrl = getQueue(sqsLocalManagerFileUpload);
		System.out.println("mySendqueue : " + mySendQueueUrl);
		myReceiveQueueUrl = getQueue(sqsManagerLocalFileDone);
		System.out.println("before creating manager instance.\n");
		Instance managerInstance = createManagerInstance(Integer.parseInt(args[args.length - 1]));

		System.out.println("myreceivequeue : " + myReceiveQueueUrl);
		System.out.println("Before createS3.\n");

		System.out.println("Sending a message to Local-Manager Queue.\n");


		/* The application will send a message to a specified
		 *  SQS queue, stating the location of the images list on S3
		 */
		sqs.sendMessage(new SendMessageRequest(sqsLocalManagerFileUpload, "new task@@@" + bucketName + "@@@" + args[0] + "@@@" + args[args.length - 1]));
		//sqs.sendMessage(new SendMessageRequest(mySendQueueUrlName, ));
		//input_output_files.put(args[0], args[(args.length - 1)/2 ]);

		ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(myReceiveQueueUrl);
		/*
		 * ****checks all messages from SQS****
		 * The application will check a specified SQS queue
		 *  for a message indicating the process is done 
		 *  and the response is available on S3.
		 */
		for(Message message : sqs.receiveMessage(receiveMessageRequest).getMessages()) {
			if(message == null)
				continue;
			else if(message.getBody().startsWith("###")) { //debug
				System.out.println(message.getBody());
				String messageRecieptHandle = message.getReceiptHandle();
				sqs.deleteMessage(new DeleteMessageRequest(myReceiveQueueUrl, messageRecieptHandle));
			}
			else {
				/*
				 * The application will download the response from S3.
				 */
				S3Object object;
				try {
					object = s3.getObject(new GetObjectRequest(bucketName, message.getBody()));
				}
				catch (Exception e) {
					continue;
				}

				//TODO: add HTML export

				String messageRecieptHandle = message.getReceiptHandle();
				sqs.deleteMessage(new DeleteMessageRequest(myReceiveQueueUrl, messageRecieptHandle));

				s3.deleteObject(bucketName, message.getBody());
			}
		}

		if(terminate) {
			sqs.sendMessage(new SendMessageRequest(mySendQueueUrl,"$$terminate"));
			boolean areDeadWorkers = false;
			while (!areDeadWorkers) {
				ReceiveMessageRequest receiveMessageRequest1 = new ReceiveMessageRequest(myReceiveQueueUrl);
				for(Message message : sqs.receiveMessage(receiveMessageRequest1).getMessages()) {
					if(message.getBody().equals("$$WorkersTerminated")) {
						List<String> instances = new ArrayList<String>();
						instances.add(managerInstance.getInstanceId());
						TerminateInstancesRequest req = new TerminateInstancesRequest(instances);
						ec2.terminateInstances(req);
						String messageRecieptHandle = message.getReceiptHandle();
						sqs.deleteMessage(new DeleteMessageRequest(myReceiveQueueUrl, messageRecieptHandle));
						areDeadWorkers = true;
					}
				}
			}
		}

		long endTime   = System.currentTimeMillis();
		long totalTime = endTime - startTime;
		System.out.println(totalTime);
		System.out.println("DONE");

	}
	private static Instance createManagerInstance(int workerCounter) {
		//Creates a new Amazon EC2 instance
		ec2 = AmazonEC2ClientBuilder.standard()
				.withCredentials(credentialsProvider)
				.withRegion("us-east-1")
				.build();
		//get list of instances from aws
		DescribeInstancesRequest request1 = new DescribeInstancesRequest();
		DescribeInstancesResult response = ec2.describeInstances(request1);
		for(Reservation res : response.getReservations()) {
			List<Instance> instances = res.getInstances();
			for(Instance in: instances) {
				for (Tag tag: in.getTags()) { 
					if(tag.getKey().equals("Manager")) {
						if(!(in.getState().getName().equals("running"))) {// starts manager instance
							if(in.getState().getName().equals("terminated") || in.getState().getName().equals("shutting-down"))
								continue;
							List<String> ins = new ArrayList<String>();
							ins.add(in.getInstanceId());
							StartInstancesRequest req = new StartInstancesRequest(ins);
							ec2.startInstances(req);
						}
						return in;
					}
				}
			}
		}

		Instance instance = null;        

		try {
			//creates manager instance  
			RunInstancesRequest request = new RunInstancesRequest("ami-0ff8a91507f77f867", 1, 1);
			//RunInstancesRequest request = new RunInstancesRequest("ami-1853ac65", 1, 1);

			request.setKeyName("test");
			request.withKeyName("test");
			request.setIamInstanceProfile(instanceP);
			//request.withSecurityGroups("Nadav");
			request.setInstanceType(InstanceType.T2Micro.toString());//request.setInstanceType(InstanceType.T2Micro.toString()); change to t2micro for better performence
			ArrayList<String> commands = new ArrayList<String>();
			commands.add("#!/bin/bash\n"); //start the bash
			commands.add("sudo su\n");
			commands.add("echo @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n");
			commands.add("echo @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n");
			commands.add("echo @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n");
			commands.add("echo @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n");
			commands.add("yum -y install java-1.8.0 \n");
			commands.add("alternatives --remove java /usr/lib/jvm/jre-1.7.0-openjdk.x86_64/bin/java\n");
			commands.add("aws configure set aws_access_key_id " + new ProfileCredentialsProvider().getCredentials().getAWSAccessKeyId());
			commands.add("aws configure set aws_secret_access_key " + new ProfileCredentialsProvider().getCredentials().getAWSSecretKey());
			commands.add("# Bootstrap: download jar from S3 and run it");
			commands.add("wget https://"+ bucketName + ".s3.amazonaws.com/" + "Manager.jar" +" -O ./" + "Manager.jar" );
			//
			//commands.add("aws s3 cp https://s3.amazonaws.com/akiajbjbasaiw6nhkk7a/Manager.jar home/ec2-user/Manager.jar");
			//commands.add("aws s3 cp s3://" + bucketName + "/Manager.jar home/ec2-user/Manager.jar");
			commands.add("java -jar Manager.jar");
			/*
			commands.add("#!/bin/bash");
			commands.add("sudo apt-get update");
			commands.add("sudo apt-get install openjdk-8-jre-headless -y");
			commands.add("aws configure set aws_access_key_id " + new ProfileCredentialsProvider().getCredentials().getAWSAccessKeyId());
			commands.add("aws configure set aws_secret_access_key " + new ProfileCredentialsProvider().getCredentials().getAWSSecretKey());
			commands.add("aws s3 cp s3://" + bucketName + "/Manager.jar home/ec2-user/Manager.jar");
			commands.add("java -jar Manager.jar");
*/

/*original
 * 
 * 			commands.add("#!/bin/bash");
			commands.add("aws configure set aws_access_key_id " + new ProfileCredentialsProvider().getCredentials().getAWSAccessKeyId());
			commands.add("aws configure set aws_secret_access_key " + new ProfileCredentialsProvider().getCredentials().getAWSSecretKey());
			commands.add("aws s3 cp s3://" + bucketName + "/Manager.jar home/ec2-user/Manager.jar");
			commands.add("yes | sudo yum install java-1.8.0");
			commands.add("yes | sudo yum remove java-1.7.0-openjdk");
			commands.add("sudo java -jar home/ec2-user/Manager.jar " + workerCounter);
		*/
			
			/* from github
			 * 		lines.add("#! /bin/bash");
		lines.add("sudo apt-get update");
		lines.add("sudo apt-get install openjdk-8-jre-headless -y");
		lines.add("sudo apt-get install wget -y");
		lines.add("sudo apt-get install unzip -y");
		lines.add("sudo wget https://s3.amazonaws.com/ass1jars203822300/manager.zip");
		lines.add("sudo unzip -P 123456 manager.zip");
		lines.add("java -jar manager.jar");
		*/

			StringBuilder builder = new StringBuilder();

			Iterator<String> commandsIterator = commands.iterator();

			while (commandsIterator.hasNext()) {
				builder.append(commandsIterator.next());
				if (!commandsIterator.hasNext()) {
					break;
				}
				builder.append("\n");
			}
			//String(Base64.encodeBase64(managerBuild.toString().getBytes()));
			String userData = new String(Base64.encodeBase64(builder.toString().getBytes()));
			request.setUserData(userData);
			//request.setUserData(createManagerScript());

			System.out.println("before running instance");
			instance = ec2.runInstances(request).getReservation().getInstances().get(0);
			System.out.println("after running instance");
			/*CreateTagsRequest request7 = new CreateTagsRequest();
			request7 = request7.withResources(instance.getInstanceId())
					.withTags(new Tag("Manager", ""));
			ec2.createTags(request7);*/
			System.out.println("Launch instance: " + instance);

		} catch (AmazonServiceException ase) {
			System.out.println("Cannot create instance : "+ ase);
		}

		return instance;
	}

	private static void createS3() {
		for (Bucket bucket : s3.listBuckets()) {
			if(bucket.getName().equals(bucketName)) {
				System.out.println("bucket.getName: (returns instead of creating bucket" + bucket.getName()); 
				return;
			}
		}

		try {
			System.out.println("Creating bucket " + bucketName + "\n");
			s3.createBucket(bucketName);
			System.out.println("S3 bucket has been created! Congratsulations");

		} catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which means your request made it "
					+ "to Amazon S3, but was rejected with an error response for some reason.");
		} catch (AmazonClientException ace) {
			System.out.println("Caught an AmazonClientException, which means the client encountered "
					+ "a serious internal problem while trying to communicate with S3, "
					+ "such as not being able to access the network.");
		}
	}
	private static String getQueue(String queueName) {
		for (String queueUrl : sqs.listQueues().getQueueUrls()) {
			if(queueName.equals(queueUrl.substring(queueUrl.lastIndexOf('/') + 1)))
				return queueUrl;
		}

		try {
			CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
			return sqs.createQueue(createQueueRequest).getQueueUrl();
		}

		catch (AmazonServiceException ase) {
			System.out.println("Caught an AmazonServiceException, which means your request made it " +
					"to Amazon SQS, but was rejected with an error response for some reason.");
		} 

		catch (AmazonClientException ace) {
			System.out.println("Error Message: " + ace.getMessage());
		}

		return null;
	}
	private static String createManagerScript() {
        StringBuilder managerBuild = new StringBuilder();
        managerBuild.append("#!/bin/bash\n"); 
        managerBuild.append("sudo su\n");
        managerBuild.append("yum -y install java-1.8.0 \n");
        managerBuild.append("alternatives --remove java /usr/lib/jvm/jre-1.7.0-openjdk.x86_64/bin/java\n");
        managerBuild.append("aws s3 cp s3://"+bucketName+"/Manager.jar  Manager.jar\n");
        managerBuild.append("java -jar Manager.jar\n");

        return new String(Base64.encodeBase64(managerBuild.toString().getBytes()));

    }
	/*private  static String build2() {
		String[] lines =new String[] {
				"#!/bin/bash",
				"sudo yum -y install java-1.8.0-openjdk.x86_64",
				"",
				"wget https://" + bucketName + ".s3amazonaws.com/" + "Manager.jar" + " -O ./" + "Manager.jar",
				"java -jar ./Manager.jar",
				"",
		};
		return new String(Base64.encode(String.join( "/n", lines).getBytes()));
	}*/
				


	//uploads 3 files, args[0] = input file , args[1] = Manager.jar file, args[2] = Worker.jar file
	private static void uploadFiles(AmazonS3 s3, String[] args) {     
		for( int i = 0; i < 3 ; i++) {
			System.out.println("Uploading jar files\n");
			String key = null;
			File file = null;

			file = new File(args[i]);
            key = file.getName().replace('\\', '_').replace('/','_').replace(':', '_');
			System.out.println("key: " + key + "\n");
			System.out.println("file: " + file + "\n");
			PutObjectRequest req = new PutObjectRequest(bucketName, key, file);
			//The application will send a message to a specified SQS queue, stating the location of the images list on S3
			s3.putObject(req);
			System.out.println("after object request");
		}
	}
}
