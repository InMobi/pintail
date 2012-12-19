package com.inmobi.messaging.consumer.util;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Calendar;
import java.util.Date;
<<<<<<< HEAD
import java.util.HashMap;
=======
>>>>>>> 2c0da5c9521b40c69a3cea585c52eea45b12e956
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import com.inmobi.databus.CheckpointProvider;

import com.inmobi.databus.files.FileMap;
import com.inmobi.databus.files.HadoopStreamFile;
import com.inmobi.databus.partition.PartitionCheckpoint;
import com.inmobi.databus.partition.PartitionCheckpointList;
import com.inmobi.databus.partition.PartitionId;
import com.inmobi.databus.readers.DatabusStreamWaitingReader;
import com.inmobi.messaging.ClientConfig;

<<<<<<< HEAD
import com.inmobi.messaging.consumer.MessageConsumerFactory;
import com.inmobi.messaging.consumer.databus.AbstractMessagingDatabusConsumer;
import com.inmobi.messaging.consumer.databus.Checkpoint;
import com.inmobi.messaging.consumer.databus.CheckpointList;
import com.inmobi.messaging.consumer.databus.DatabusConsumer;
import com.inmobi.messaging.consumer.databus.DatabusConsumerConfig;
import com.inmobi.messaging.consumer.databus.StreamType;
import com.inmobi.messaging.consumer.hadoop.HadoopConsumer;
import com.inmobi.messaging.consumer.hadoop.HadoopConsumerConfig;
=======
import com.inmobi.messaging.consumer.databus.Checkpoint;
import com.inmobi.messaging.consumer.databus.CheckpointList;
import com.inmobi.messaging.consumer.databus.DatabusConsumerConfig;
import com.inmobi.messaging.consumer.databus.StreamType;
>>>>>>> 2c0da5c9521b40c69a3cea585c52eea45b12e956


public class CheckpointUtil implements DatabusConsumerConfig {

  private static final Log LOG = LogFactory.getLog(CheckpointUtil.class);
<<<<<<< HEAD

  public static void prepareCheckpointList(String superKey,
      CheckpointProvider provider, Set<Integer> idList,
      Map<PartitionId, Path> streamDirs,
      CheckpointList checkpointList) 
          throws IOException {
=======
  
  public CheckpointUtil() {
  	
  }

  public static void prepareCheckpointList(String superKey,
      CheckpointProvider provider,
      Set<Integer> idList, Path streamDir, CheckpointList checkpointList) 
      		throws IOException {

>>>>>>> 2c0da5c9521b40c69a3cea585c52eea45b12e956
    Checkpoint oldCheckpoint = null;
    byte[] chkpointData = provider.read(superKey);
    if (chkpointData != null) {
      oldCheckpoint = new Checkpoint(chkpointData);
    }

    if (oldCheckpoint == null) {
      LOG.info("Old checkpoint is not available nothing to prepare");
      return;
    }
<<<<<<< HEAD

=======
   
>>>>>>> 2c0da5c9521b40c69a3cea585c52eea45b12e956
    Map<PartitionId, PartitionCheckpoint> partitionCheckpoints = 
        oldCheckpoint.getPartitionsCheckpoint();
    for (Map.Entry<PartitionId, PartitionCheckpoint> entry : 
      partitionCheckpoints.entrySet()) {
<<<<<<< HEAD
      PartitionId pid = entry.getKey();
      Path streamDir = streamDirs.get(pid);
      HadoopStreamFile streamFile = (HadoopStreamFile)entry.getValue().getStreamFile();
      Map<Integer, PartitionCheckpoint> thisChkpoint =
          new TreeMap<Integer, PartitionCheckpoint>();
      Calendar chkCal = Calendar.getInstance();
      Date checkpointDate = DatabusStreamWaitingReader.getDateFromCheckpointPath(
          streamFile.getCheckpointPath());
      chkCal.setTime(checkpointDate);
      int checkpointMin = chkCal.get(Calendar.MINUTE);
      Calendar chkPrevHrCal = Calendar.getInstance();
      chkPrevHrCal.setTime(checkpointDate);
      chkPrevHrCal.add(Calendar.MINUTE, 1);
      chkPrevHrCal.add(Calendar.HOUR, -1);
      while (chkPrevHrCal.before(chkCal)) {
        if (chkPrevHrCal.get(Calendar.MINUTE) == 0) {
          break;
        }
        Path minDir = DatabusStreamWaitingReader.getMinuteDirPath(streamDir,
            chkPrevHrCal.getTime());
        FileStatus lastElement = getLast(minDir);
        if (lastElement != null) {
          thisChkpoint.put(chkPrevHrCal.get(Calendar.MINUTE),
              new PartitionCheckpoint(new HadoopStreamFile(
                  lastElement.getPath().getParent(),
                  lastElement.getPath().getName(),
                  lastElement.getModificationTime()),
                  -1));
        }
        chkPrevHrCal.add(Calendar.MINUTE, 1);
      }        

      Calendar chkHrCal = Calendar.getInstance();
      chkHrCal.setTime(checkpointDate);
      chkHrCal.set(Calendar.MINUTE, 0);
      while (chkHrCal.before(chkCal)) {
        Path minDir = DatabusStreamWaitingReader.getMinuteDirPath(streamDir,
            chkHrCal.getTime());
        FileStatus lastElement = getLast(minDir);
        if (lastElement != null) {
          thisChkpoint.put(chkHrCal.get(Calendar.MINUTE),
              new PartitionCheckpoint(new HadoopStreamFile(
                  lastElement.getPath().getParent(),
                  lastElement.getPath().getName(),
                  lastElement.getModificationTime()),
                  -1));
        }
        chkHrCal.add(Calendar.MINUTE, 1);
      }

      thisChkpoint.put(checkpointMin, entry.getValue());
      checkpointList.setForCheckpointUtil(entry.getKey(), new 
          PartitionCheckpointList(thisChkpoint));
    }
  }


=======
    	Path tmpPathFile = new Path(entry.getValue().getStreamFile().toString()).
    			getParent();
    	
    	if ((tmpPathFile.toString()).contains(streamDir.toString())) {
    		Map<Integer, PartitionCheckpoint> thisChkpoint =
    				new TreeMap<Integer, PartitionCheckpoint>();
    		Calendar chkCal = Calendar.getInstance();
    		Date checkpointDate = DatabusStreamWaitingReader.getDateFromStreamDir(
    				streamDir,
    				new Path(entry.getValue().getStreamFile().toString()).getParent());
    		chkCal.setTime(checkpointDate);
    		int checkpointMin = chkCal.get(Calendar.MINUTE);
    		Calendar chkPrevHrCal = Calendar.getInstance();
    		chkPrevHrCal.setTime(checkpointDate);
    		chkPrevHrCal.add(Calendar.MINUTE, 1);
    		chkPrevHrCal.add(Calendar.HOUR, -1);
    		while (chkPrevHrCal.before(chkCal)) {
    			if (chkPrevHrCal.get(Calendar.MINUTE) == 0) {
    				break;
    			}
    			Path minDir = DatabusStreamWaitingReader.getMinuteDirPath(streamDir,
    					chkPrevHrCal.getTime());
    			FileStatus lastElement = getLast(minDir);
    			if (lastElement != null) {
    				thisChkpoint.put(chkPrevHrCal.get(Calendar.MINUTE),
    						new PartitionCheckpoint(new HadoopStreamFile(
    								lastElement.getPath().getParent(),
    								lastElement.getPath().getName(),
    								lastElement.getModificationTime()),
    								-1));
    			}
    			chkPrevHrCal.add(Calendar.MINUTE, 1);
    		}        

    		Calendar chkHrCal = Calendar.getInstance();
    		chkHrCal.setTime(checkpointDate);
    		chkHrCal.set(Calendar.MINUTE, 0);
    		while (chkHrCal.before(chkCal)) {
    			Path minDir = DatabusStreamWaitingReader.getMinuteDirPath(streamDir,
    					chkHrCal.getTime());
    			FileStatus lastElement = getLast(minDir);
    			if (lastElement != null) {
    				thisChkpoint.put(chkHrCal.get(Calendar.MINUTE),
    						new PartitionCheckpoint(new HadoopStreamFile(
    								lastElement.getPath().getParent(),
    								lastElement.getPath().getName(),
    								lastElement.getModificationTime()),
    								-1));
    			}
    			chkHrCal.add(Calendar.MINUTE, 1);
    		}
    		
    		thisChkpoint.put(checkpointMin, entry.getValue());
    		checkpointList.setForCheckpointUtil(entry.getKey(), new PartitionCheckpointList(
    				thisChkpoint));
    	}
    }
  }

>>>>>>> 2c0da5c9521b40c69a3cea585c52eea45b12e956
  private static FileStatus getLast(final Path minDir)
      throws IOException {

    FileMap<HadoopStreamFile> fmap = new FileMap<HadoopStreamFile>() {
      @Override
      protected TreeMap<HadoopStreamFile, FileStatus> createFilesMap() {
        return new TreeMap<HadoopStreamFile, FileStatus>();
      }

      @Override
      protected HadoopStreamFile getStreamFile(String fileName) {
        throw new RuntimeException("Not implemented");
      }

      @Override
      protected HadoopStreamFile getStreamFile(FileStatus file) {
        return HadoopStreamFile.create(file);
      }

      @Override
      protected PathFilter createPathFilter() {
        return new PathFilter() {
          @Override
          public boolean accept(Path path) {
            if (path.getName().startsWith("_")) {
              return false;
            }
            return true;
          }          
        };
      }

      @Override
      protected void buildList() throws IOException {
        FileSystem fs = minDir.getFileSystem(new Configuration());
        doRecursiveListing(fs, minDir, pathFilter, this);
      }
    };

    fmap.build();

    return fmap.getLastFile();
  }

  private static void doRecursiveListing(FileSystem fs, Path dir,
      PathFilter pathFilter,
      FileMap<HadoopStreamFile> fmap) throws IOException {
    FileStatus[] fileStatuses = fs.listStatus(dir, pathFilter);
    if (fileStatuses == null || fileStatuses.length == 0) {
      LOG.debug("No files in directory:" + dir);
    } else {
      for (FileStatus file : fileStatuses) {
        if (file.isDir()) {
          doRecursiveListing(fs, file.getPath(), pathFilter, fmap);
        } else {
          fmap.addPath(file);
        }
      }
    }
  }
<<<<<<< HEAD

  public static void run(String args[]) throws Exception {
    String confFile = args[0];
    ClientConfig config;
    CheckpointProvider checkpointProvider;
    Set<Integer> idList = new TreeSet<Integer>();
    config = ClientConfig.load(confFile);
    String chkpointProviderClassName = config.getString(
        chkProviderConfig, DEFAULT_CHK_PROVIDER);
    String databusCheckpointDir = config.getString(checkpointDirConfig, 
        DEFAULT_CHECKPOINT_DIR);
    checkpointProvider = AbstractMessagingDatabusConsumer.createCheckpointProvider(
        chkpointProviderClassName, databusCheckpointDir);

    for (int i = 0; i < 60; i++) {
      idList.add(i);
    }
    String topicName = config.getString(MessageConsumerFactory.TOPIC_NAME_KEY);
    String consumerName = config.getString(MessageConsumerFactory.CONSUMER_NAME_KEY);
    String superKey = consumerName + "_" + topicName;
    String rootDirConfig;
    Map<PartitionId, Path> streamDirs = new HashMap<PartitionId, Path>();
    String[] rootDirs;
    if (config.getString(databusRootDirsConfig) != null) {
      String type = config.getString(databusStreamType, StreamType.COLLECTOR.name());
      StreamType streamType = StreamType.valueOf(type);
      if (streamType.equals(StreamType.COLLECTOR)) {
        LOG.info("No migration required");
        return;
      }
      rootDirConfig = config.getString(databusRootDirsConfig);
      rootDirs = rootDirConfig.split(",");
      int i = 0;
      for (String rootDir : rootDirs) {
        Path streamDir = DatabusUtil.getStreamDir(streamType, 
            new Path(rootDir), topicName);
        streamDirs.put(new PartitionId(DatabusConsumer.clusterNamePrefix + i, 
            null), streamDir);
        i++;
      }
    } else if (config.getString(HadoopConsumerConfig.rootDirsConfig) != null) {
      rootDirConfig = config.getString(HadoopConsumerConfig.rootDirsConfig);
      rootDirs = rootDirConfig.split(",");
      int i = 0;
      for (String rootDir : rootDirs) {
        Path streamDir = new Path(rootDir);
        streamDirs.put(new PartitionId(HadoopConsumer.clusterNamePrefix + i, 
            null), streamDir);
        i++;
      }
    }
    CheckpointList checkpointList = new CheckpointList(idList);
    CheckpointUtil.prepareCheckpointList(superKey, checkpointProvider, idList,
        streamDirs, checkpointList);
    checkpointList.write(checkpointProvider, superKey);
    checkpointList.read(checkpointProvider, superKey); 
  }

  public static void main(String [] args) throws Exception {
    if (args.length == 1) {
      run(args);
    } else {
      System.out.println("incorrect number of arguments. provide one argument : " +
          "path to configuration file ");
      System.exit(1);
    }
=======
  
  protected static CheckpointProvider createCheckpointProvider(
  	String checkpointProviderClassName, String chkpointDir) {
  	CheckpointProvider chkProvider = null;
  	try {
  		Class<?> clazz = Class.forName(checkpointProviderClassName);
  		Constructor<?> constructor = clazz.getConstructor(String.class);
  		chkProvider = (CheckpointProvider) constructor.newInstance(new Object[]
  				{chkpointDir});
  	} catch (Exception e) {
  		throw new IllegalArgumentException("Could not create checkpoint provider "
  				+ checkpointProviderClassName, e);
  	}
  	return chkProvider;
  }
  
  public static void run(String args[]) throws Exception {
  		String confFile = args[0];
  		ClientConfig config;
  		CheckpointProvider checkpointProvider;
  		Set<Integer> idList = new TreeSet<Integer>();
  		config = ClientConfig.load(confFile);
  		String className = config.getString("consumer.classname");
  		String chkpointProviderClassName = config.getString(
  				chkProviderConfig, DEFAULT_CHK_PROVIDER);
  		String databusCheckpointDir = config.getString(checkpointDirConfig, 
  				DEFAULT_CHECKPOINT_DIR);
  		checkpointProvider = createCheckpointProvider(
  				chkpointProviderClassName, databusCheckpointDir);

  		for (int i = 0; i < 60; i++) {
  			idList.add(i);
  		}
  		String topicName = config.getString("topic.name", null);
  		String consumerName = config.getString("consumer.name", null);
  		String type = config.getString(databusStreamType, "LOCAL");
  		String [] databusRootDir = null;
  		if (className.compareTo(
  				"com.inmobi.messaging.consumer.databus.DatabusConsumer") == 0) {
  			databusRootDir = (config.getString(databusRootDirsConfig)).split(",");
  		} else if (className.compareTo(
					"com.inmobi.messaging.consumer.hadoop.HadoopConsumer") == 0) {
  			databusRootDir = (config.getString("hadoop.consumer.rootdirs")).split(",");
  		} else {
				System.out.println("mention consumer class name in the conf file");
				System.exit(1);
			}
  		StreamType streamType = StreamType.valueOf(type);
  		String superKey = consumerName + "_" + topicName;
  		CheckpointList checkpointList = new CheckpointList(idList);
  		Path streamDir = null;
  		for (String databusRootDirPath : databusRootDir) {
  			if (className.compareTo(
  					"com.inmobi.messaging.consumer.databus.DatabusConsumer") == 0) {
  				streamDir = DatabusUtil.getStreamDir(streamType, 
  						new Path(databusRootDirPath), topicName);
  			} else {
  				streamDir = new Path(databusRootDirPath);
  			} 
  			CheckpointUtil.prepareCheckpointList(superKey, checkpointProvider, idList,
  					streamDir, checkpointList);
  		}
  		checkpointList.write(checkpointProvider, superKey);
  		checkpointList.read(checkpointProvider, superKey); 
  }

  public static void main(String [] args) throws Exception {
  	if (args.length == 1) {
  		run(args);
  	} else {
  		System.out.println("incorrect number of arguments. provide one argument : " +
  				"path to configuration file ");
  		System.exit(1);
  	}
>>>>>>> 2c0da5c9521b40c69a3cea585c52eea45b12e956
  }
}


