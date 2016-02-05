package org.apache.flume.source.taildir;

public class TaildirSourceConfigurationConstants {
  /** Mapping for tailing file groups. */
  public static final String FILE_GROUPS = "filegroups";
  public static final String FILE_GROUPS_PREFIX = FILE_GROUPS + ".";

  /** Mapping for putting headers to events grouped by file groups. */
  public static final String HEADERS_PREFIX = "headers.";

  /** Path of position file. */
  public static final String POSITION_FILE = "positionFile";
  public static final String DEFAULT_POSITION_FILE = "~/taildir_position.json";
  public static final String STOP_FLAG_FILE = "stopflag";
  public static final String RUNNING_FLAG_FILE = "runningflag";
  public static final String DEFAULT_STOP_FLAG_FILE = "./stop.flag";
  /** What size to batch with before sending to ChannelProcessor. */
  public static final String BATCH_SIZE = "batchSize";
  public static final int DEFAULT_BATCH_SIZE = 100;

  /** Whether to skip the position to EOF in the case of files not written on the position file. */
  public static final String SKIP_TO_END = "skipToEnd";
  public static final boolean DEFAULT_SKIP_TO_END = false;

  /** Time (ms) to close idle files. */
  public static final String IDLE_TIMEOUT = "idleTimeout";
  public static final int DEFAULT_IDLE_TIMEOUT = 300000;
  
  public static final String RETRY_INTERVEL = "retryInterval";
  public static final int DEFAULT_RETRY_INTERVAL = 50;
  /**
   * Times to stop.
   */
  public static final String IDLE_MAX_TIMES = "idleMaxTimes";
  
  /** Interval time (ms) to write the last position of each file on the position file. */
  public static final String WRITE_POS_INTERVAL = "writePosInterval";
  public static final int DEFAULT_WRITE_POS_INTERVAL = 3000;
  
  /** Whether to add the byte offset of a tailed line to the header */
  public static final String BYTE_OFFSET_HEADER = "byteOffsetHeader";
  public static final String BYTE_OFFSET_HEADER_KEY = "byteoffset";
  public static final boolean DEFAULT_BYTE_OFFSET_HEADER = false;
}