package com.oscargreat.cloud.stormhm.mq;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

public class MQClient{

	private static final String MQ_SERVERHOST = "localhost";

	public static Logger LOG = Logger.getLogger(MQClient.class);

	ConnectionFactory mqfactory;
	Connection mqconnection;
	Channel mqchannel;
	QueueingConsumer consumer;
	String queueName;
	private CallbackThread thread;

	public MQClient() {
		mqfactory = new ConnectionFactory();
		mqfactory.setHost(MQ_SERVERHOST);
	}

	public void connect(String queueName) throws IOException {
		if (mqconnection != null && mqconnection.isOpen())
			mqconnection.close();
		mqconnection = mqfactory.newConnection();
		mqchannel = mqconnection.createChannel();
		mqchannel.queueDeclare(queueName, false,false,false,null);
		consumer = new QueueingConsumer(mqchannel);
		mqchannel.basicConsume(queueName, true, consumer);
		this.queueName = queueName;
	}

	public void disconnect() throws IOException {
		mqconnection.close();
	}

	/**
	 * block RPC call
	 * 
	 * @param queueName
	 * @param message
	 * @return
	 * @throws IOException
	 */
	public String syncCall(String message) throws IOException {
		String response = null;
		String corrId = java.util.UUID.randomUUID().toString();
		String replyQueueName = mqchannel.queueDeclare().getQueue();
		BasicProperties props = new BasicProperties.Builder()
				.correlationId(corrId).replyTo(replyQueueName).build();

		mqchannel.basicPublish("", queueName, props, message.getBytes());

		try {
			while (true) {
				QueueingConsumer.Delivery delivery = consumer.nextDelivery();
				if (delivery.getProperties().getCorrelationId().equals(corrId)) {
					response = new String(delivery.getBody());
					break;
				}
			}
		} catch (Exception e) {
			// TODO: handle exception
		}

		return response;

	}

	public void startWaitCall(String queueName, IRPCCallee callee){

		thread = new CallbackThread(this, callee, queueName);
		thread.start();
	}

	public void stopWaitCall() {
		thread.stopRun();
	}

	public boolean isConnected() {
		return mqconnection != null && mqconnection.isOpen();
	}


	public void reply(CallbackContext context, String response) throws IOException {
		BasicProperties replyProps = new BasicProperties.Builder()
				.correlationId(context.getCallID()).build();

		mqchannel.basicPublish("", context.getReplyQueue(), replyProps,
				response.getBytes());

		mqchannel.basicAck(context.getDeliveryTag(), false);

	}

}
