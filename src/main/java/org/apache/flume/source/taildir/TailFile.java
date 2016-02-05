package org.apache.flume.source.taildir;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

public class TailFile {
  private static final Logger logger = LoggerFactory.getLogger(TailFile.class);

  private static final String LINE_SEP = System.lineSeparator();
  private static final char LINE_END = LINE_SEP.charAt(LINE_SEP.length() - 1);
  private RandomAccessFile raf;
  private final String path;
  private final long inode;
  private long pos;
  private long lastUpdated;
  private boolean needTail;
  //private final Map<String, String> headers;

  public TailFile(File file, long inode, long pos)
      throws IOException {
    this.raf = new RandomAccessFile(file, "r");
    if (pos > 0) raf.seek(pos);
    this.path = file.getAbsolutePath();
    this.inode = inode;
    this.pos = pos;
    this.lastUpdated = 0L;
    this.needTail = true;
    //App.getOs().equalsIgnoreCase("windows");
  }

  public RandomAccessFile getRaf() { return raf; }
  public String getPath() { return path; }
  public long getInode() { return inode; }
  public long getPos() { return pos; }
  public long getLastUpdated() { return lastUpdated; }
  public boolean needTail() { return needTail; }
  //public Map<String, String> getHeaders() { return headers; }

  public void setPos(long pos) { this.pos = pos; }
  public void setLastUpdated(long lastUpdated) { this.lastUpdated = lastUpdated; }
  public void setNeedTail(boolean needTail) { this.needTail = needTail; }

  public boolean updatePos(String path, long inode, long pos) throws IOException {
    if (this.inode == inode && this.path.equals(path)) {
      raf.seek(pos);
      setPos(pos);
      logger.info("Updated position, file: " + path + ", inode: " + inode + ", pos: " + pos);
      return true;
    }
    return false;
  }

  public List<String> readEvents(int numEvents, boolean backoffWithoutNL) throws IOException {
    List<String> events = Lists.newLinkedList();
    for (int i = 0; i < numEvents; i++) {
    	String event = readEvent(backoffWithoutNL);
      if (event == null) {
        break;
      }
      events.add(event);
    }
    return events;
  }

  private String readEvent(boolean backoffWithoutNL) throws IOException {
    Long posTmp = raf.getFilePointer();
    String line = readLine();
    if (line == null) {
      return null;
    }
    if (backoffWithoutNL && !line.endsWith(LINE_SEP)) {
      logger.info("Backoffing in file without newline: "
          + path + ", inode: " + inode + ", pos: " + raf.getFilePointer());
      raf.seek(posTmp);
      return null;
    }

    //StringEvent event = new StringEvent(StringUtils.removeEnd(line, LINE_SEP).getBytes(Charsets.UTF_8));
    /*if (addByteOffset == true) {
      event.getHeaders().put(BYTE_OFFSET_HEADER_KEY, posTmp.toString());
    }*/
    /**去掉UTF-8 BOM头的代码先屏蔽，因数据方已修改编码为标准utf-8
    if (line.startsWith("\uFEFF")){
    	line = StringUtils.removeStart(line, "\uFEFF");
    }*/
    return StringUtils.removeEnd(line, LINE_SEP);
  }

  private String readLine() throws IOException {
    ByteArrayDataOutput out = ByteStreams.newDataOutput(620);
    int i = 0;
    int c;
    while ((c = raf.read()) != -1) {
      i++;
      out.write((byte) c);
      if (c == LINE_END) {
        break;
      }
    }
    if (i == 0) {
      return null;
    }
    return new String(out.toByteArray(), Charsets.UTF_8);
  }

  public void close() {
    try {
      raf.close();
      raf = null;
      long now = System.currentTimeMillis();
      setLastUpdated(now);
    } catch (IOException e) {
      logger.error("Failed closing file: " + path + ", inode: " + inode, e);
    }
  }
}