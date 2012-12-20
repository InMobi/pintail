package com.inmobi.messaging.consumer.databus;

import java.util.Set;

public interface MessageCheckpoint {

	boolean isNULL(Set<Integer> partitionMinList);

}

