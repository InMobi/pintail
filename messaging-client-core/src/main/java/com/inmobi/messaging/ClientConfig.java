package com.inmobi.messaging;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

public class ClientConfig {

  private Map<String, String> params = new HashMap<String, String>();

  public ClientConfig(Map<String, String> params) {
    this.params = params;
  }

  public static ClientConfig load(String confFile) {
    try {
      return load(new FileInputStream(new File(confFile)));
    } catch (FileNotFoundException e) {
      throw new RuntimeException("could not load conf file " + confFile, e);
    }
  }

  public static ClientConfig load(InputStream in) {
    return null;
  }

  public static ClientConfig load() {
    InputStream in = null;//via classpath
    return load(in);
  }

  public Boolean getBoolean(String key, Boolean defaultValue) {
    String value = get(key);
    if(value != null) {
      return Boolean.parseBoolean(value.trim());
    }
    return defaultValue;
  }
  
  public Boolean getBoolean(String key) {
    return getBoolean(key, null);
  }
  
  public Integer getInteger(String key, Integer defaultValue) {
    String value = get(key);
    if(value != null) {
      return Integer.parseInt(value.trim());
    }
    return defaultValue;
  }
  
  public Integer getInteger(String key) {
    return getInteger(key, null);
  }
  
  public Long getLong(String key, Long defaultValue) {
    String value = get(key);
    if(value != null) {
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
    if(result != null) {
      return result;
    }
    return defaultValue;
  }
  private String get(String key) {
    return get(key, null);
  }
}
