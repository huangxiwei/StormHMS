package com.oscargreat.cloud.stormhm.mq;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.rabbitmq.client.QueueingConsumer;
import com.rabbitmq.client.ShutdownSignalException;
import com.rabbitmq.client.AMQP.BasicProperties;


public class CallbackThread extends Thread {
	private MQClient client;
	private IRPCCallee callee;
	private String queueName;
	private boolean runningThread = false;
	public static Logger LOG = Logger.getLogger(MQClient.class);
	
	public CallbackThread(MQClient owner, IRPCCallee callee, String queueName) {
		client = owner;
		this.callee = callee;
		this.queueName = queueName;
	}
	
	public void stopRun(){
		runningThread = false;
		this.interrupt();
	}
	
	@Override
	public void run() {
		runningThread = true;
		while (runningThread) {
			QueueingConsumer consumer;
			try {
				client.connect(queueName);
				client.mqchannel.queueDeclare(queueName, false, false, false, null);

				client.mqchannel.basicQos(1);

				consumer= new QueueingConsumer(client.mqchannel);
				client.mqchannel.basicConsume(queueName, false, consumer);
				
			} catch (IOException e1) {
				LOG.error("cannot connect to MQ server");
				try {
					Thread.sleep(3000);
				} catch (InterruptedException e) {
				}
				continue;
			}
			
			while (runningThread) {
				try {
				 QueueingConsumer.Delivery delivery = consumer.nextDelivery();

				    BasicProperties props = delivery.getProperties();
				    String message = new String(delivery.getBody());

				    callee.remotedCalled(client, new CallbackContext(props.getReplyTo(), props.getCorrelationId(), delivery.getEnvelope().getDeliveryTag(), message));
				} catch (InterruptedException e) {

				} catch (ShutdownSignalException e) {
					LOG.warn("connection lost. try reconnect to MQ server...");
					break;// to restart connection
				}
			}

		}

	}
};