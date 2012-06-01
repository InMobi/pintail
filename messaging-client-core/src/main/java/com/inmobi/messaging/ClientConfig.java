package com.inmobi.messaging;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Provides access to Configuration parameters for the messaging client.
 * 
 * Configuration parameters are defined as name, value pairs.
 * 
 */
public class ClientConfig {

  private final Map<String, String> params = new HashMap<String, String>();

  /**
   * Create a new client configuration.
   */
  public ClientConfig() {
  }

  /**
   * Create a new client configuration with passed parameters
   * 
   * @param parameters The {@link Map} holding configuration parameters
   */
  public ClientConfig(Map<String, String> parameters) {
    this.params.putAll(parameters);
  }

  /**
   * Load configuration from of the passed filename from the classpath.
   * 
   * The data is in a simple line-oriented format as in 
   * {@link Properties#load(InputStream)}

   * @param confFile The file name to be loaded
   * 
   * @return The loaded {@link ClientConfig} object
   */
  public static ClientConfig loadFromClasspath(String confFile) {
    InputStream in = ClientConfig.class.getClassLoader().getResourceAsStream(
        confFile);
    if (in == null) {
      throw new RuntimeException("could not load conf file "
          + confFile + " from classpath.");
    }
    return ClientConfig.load(in);
  }

  /**
   * Load configuration from the passed configuration file.
   * 
   * The data is in a simple line-oriented format as in 
   * {@link Properties#load(InputStream)}

   * @param confFile The file name to be loaded
   * 
   * @return The loaded {@link ClientConfig} object
   */
  public static ClientConfig load(String confFile) {
    try {
      return load(new FileInputStream(new File(confFile)));
    } catch (FileNotFoundException e) {
      throw new RuntimeException("could not load conf file " + confFile, e);
    }
  }

  /**
   * Load configuration parameters from the passed input stream.
   * 
   * The input stream is in a simple line-oriented format as in 
   * {@link Properties#load(InputStream)}
   * 
   * @param in The {@link InputStream} to be loaded
   * 
   * @return The loaded {@link ClientConfig} object
   */
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

  /**
   * Set a configuration parameter with passed name and value
   * 
   * @param name  {@link String} parameter name
   * @param value  {@link String} parameter value
   */
  public void set(String name, String value) {
    params.put(name, value);
  }

  /**
   * Get the boolean value associated with passed parameter <code>name</code>.
   * If no such name exists in configuration parameters, the default value
   * passed will be returned
   *  
   * @param name {@link String} configuration parameter name
   * @param defaultValue {@link Boolean} default value if the name does not
   * exist in configuration parameters.
   *  
   * @return {@link Boolean}
   */
  public Boolean getBoolean(String name, Boolean defaultValue) {
    String value = get(name);
    if (value != null) {
      return Boolean.parseBoolean(value.trim());
    }
    return defaultValue;
  }

  /**
   * Get the boolean value associated with passed parameter <code>name</code>
   *  
   * @param name {@link String} configuration parameter name.
   *  
   * @return {@link Boolean} object
   */
  public Boolean getBoolean(String name) {
    return getBoolean(name, null);
  }

  /**
   * Get the integer value associated with passed parameter <code>name</code>
   * If no such name exists in configuration parameters, the default value
   * passed will be returned
   *  
   * @param name {@link String} configuration parameter name
   * @param defaultValue {@link Integer} default value if the name does not
   * exist in configuration parameters.
   *  
   * @return {@link Integer}
   */
  public Integer getInteger(String name, Integer defaultValue) {
    String value = get(name);
    if (value != null) {
      return Integer.parseInt(value.trim());
    }
    return defaultValue;
  }

  /**
   * Get the integer value associated with passed parameter <code>name</code>
   *  
   * @param name {@link String} configuration parameter name
   *  
   * @return {@link Integer} object
   */
  public Integer getInteger(String name) {
    return getInteger(name, null);
  }

  /**
   * Get the long value associated with passed parameter <code>name</code>
   * If no such name exists in configuration parameters, the default value
   * passed will be returned
   *  
   * @param name {@link String} configuration parameter name
   * @param defaultValue {@link Long} default value if the name does not
   * exist in configuration parameters.
   *  
   * @return {@link Long} object
   */
  public Long getLong(String name, Long defaultValue) {
    String value = get(name);
    if (value != null) {
      return Long.parseLong(value.trim());
    }
    return defaultValue;
  }

  /**
   * Get the long value associated with passed parameter <code>name</code>
   *  
   * @param name {@link String} configuration parameter name
   *  
   * @return {@link Long} object
   */
  public Long getLong(String name) {
    return getLong(name, null);
  }

  /**
   * Get the value associated with passed parameter <code>name</code>
   * If no such name exists in configuration parameters, the default value
   * passed will be returned
   *  
   * @param name {@link String} configuration parameter name
   * @param defaultValue {@link String} default value if the name does not
   * exist in configuration parameters.
   *  
   * @return {@link String} object
   */
  public String getString(String name, String defaultValue) {
    return get(name, defaultValue);
  }

  /**
   * Get the value associated with passed parameter <code>name</code>
   *  
   * @param name {@link String} configuration parameter name
   *  
   * @return {@link String} object
   */
  public String getString(String name) {
    return get(name);
  }

  private String get(String name, String defaultValue) {
    String result = params.get(name);
    if (result != null) {
      return result;
    }
    return defaultValue;
  }

  private String get(String name) {
    return get(name, null);
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
