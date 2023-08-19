package service;

import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.*;

public class SQSService {
    public static AmazonSQS createSQSClient(String region) {
        return AmazonSQSClient.builder()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration("http://localhost:4566", region))
                .build();
    }
    public static void main(String[] args) {

        String queueName = "queue1";
        String message = "Hello, This is a message";

        String messageId = publishMessageToSQS(queueName, message);

        System.out.println("Message published to SQS with message ID: " + messageId);

        receiveAndProcessMessagesFromSQS(queueName);
    }

    public static String publishMessageToSQS(String queueName, String message) {
        AmazonSQS sqsClient = createSQSClient("us-east-2"); // Specify the desired region


        CreateQueueRequest createQueueRequest = new CreateQueueRequest(queueName);
        String queueUrl = sqsClient.createQueue(createQueueRequest).getQueueUrl();

        SendMessageRequest sendMessageRequest = new SendMessageRequest(queueUrl, message);
        SendMessageResult sendMessageResult = sqsClient.sendMessage(sendMessageRequest);

        return sendMessageResult.getMessageId();
    }

    public static void receiveAndProcessMessagesFromSQS(String queueName) {
        AmazonSQS sqsClient = createSQSClient("us-east-2");



        GetQueueUrlResult getQueueUrlResult = sqsClient.getQueueUrl(queueName);

        String queueUrl = getQueueUrlResult.getQueueUrl();

        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl)
                .withMaxNumberOfMessages(10)
                .withWaitTimeSeconds(20);

        ReceiveMessageResult receiveMessageResult = sqsClient.receiveMessage(receiveMessageRequest);

        for (Message msg : receiveMessageResult.getMessages()) {
            System.out.println("Received Message: " + msg.getBody());
            sqsClient.deleteMessage(queueUrl, msg.getReceiptHandle());
        }
    }
}
