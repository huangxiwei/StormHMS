package com.oscargreat.cloud.stormhm.mq;

public interface IRPCCallee {
	void remotedCalled(MQClient client, CallbackContext context);
}
