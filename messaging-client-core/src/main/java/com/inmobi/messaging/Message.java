package com.inmobi.messaging;

import java.util.Arrays;

public final class Message {
	
	private final String topic;
	private final byte message[];
	
	public Message(String topic, byte message[]) {
		this.topic = topic;
		this.message = message;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(message);
		result = prime * result + ((topic == null) ? 0 : topic.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Message other = (Message) obj;
		if (!Arrays.equals(message, other.message))
			return false;
		if (topic == null) {
			if (other.topic != null)
				return false;
		} else if (!topic.equals(other.topic))
			return false;
		return true;
	}
	
	public String getTopic() {
		return topic;
	}
	
	public byte[] getMessage() {
		return message;
	}
}
