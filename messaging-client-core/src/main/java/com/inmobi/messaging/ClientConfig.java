package com.inmobi.messaging;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientConfig {

  private static final Logger LOG = LoggerFactory.getLogger(ClientConfig.class);
  public static final String MESSAGE_CLIENT_CONF_FILE = "messaging-client-conf.properties";
  public static final String PUBLISHER_CLASS_NAME_KEY = "publisher.classname";
  public static final String EMITTER_CONF_FILE_KEY = "statemitter.filename";

  private final Map<String, String> params = new HashMap<String, String>();

  public ClientConfig() {
  }

  public ClientConfig(Map<String, String> map) {
    this.params.putAll(map);
  }

  public static ClientConfig load(String confFile) {
    try {
      return load(new FileInputStream(new File(confFile)));
    } catch (FileNotFoundException e) {
      throw new RuntimeException("could not load conf file " + confFile, e);
    }
  }

  public static ClientConfig load(InputStream in) {
    Properties props = new Properties();
    try {
      props.load(in);
    } catch (IOException e) {
      throw new RuntimeException("could not load conf", e);
    }
    Map<String, String> map = new HashMap<String, String>();
    for (Object key : props.keySet()) {
      String k = (String) key;
      String v = props.getProperty(k);
      map.put(k, v);
    }
    return new ClientConfig(map);
  }

  /*
   * Loads the config by looking the config file {@link
   * #MESSAGE_CLIENT_CONF_FILE} in the classpath.
   */
  public static ClientConfig load() {
    InputStream in = ClientConfig.class
        .getClassLoader().getResourceAsStream(MESSAGE_CLIENT_CONF_FILE);
    if (in == null) {
      throw new RuntimeException("could not load conf file " + 
          MESSAGE_CLIENT_CONF_FILE + " from classpath.");
    }
    return load(in);
  }

  public void set(String key, String value) {
    params.put(key, value);
  }

  public Boolean getBoolean(String key, Boolean defaultValue) {
    String value = get(key);
    if (value != null) {
      return Boolean.parseBoolean(value.trim());
    }
    return defaultValue;
  }

  public Boolean getBoolean(String key) {
    return getBoolean(key, null);
  }

  public Integer getInteger(String key, Integer defaultValue) {
    String value = get(key);
    if (value != null) {
      return Integer.parseInt(value.trim());
    }
    return defaultValue;
  }

  public Integer getInteger(String key) {
    return getInteger(key, null);
  }

  public Long getLong(String key, Long defaultValue) {
    String value = get(key);
    if (value != null) {
      return Long.parseLong(value.trim());
    }
    return defaultValue;
  }

  public Long getLong(String key) {
    return getLong(key, null);
  }

  public String getString(String key, String defaultValue) {
    return get(key, defaultValue);
  }

  public String getString(String key) {
    return get(key);
  }

  private String get(String key, String defaultValue) {
    String result = params.get(key);
    if (result != null) {
      return result;
    }
    return defaultValue;
  }

  private String get(String key) {
    return get(key, null);
  }

  @Override
  public String toString() {
    StringBuffer sb = new StringBuffer();
    for (Map.Entry<String, String> entry : params.entrySet()) {
      sb.append(entry.getKey());
      sb.append(":");
      sb.append(entry.getValue());
      sb.append("\n");
    }
    return sb.toString();
  }
}
