package org.apache.flume.source.taildir;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gaea.tailsource.common.CommonUtil;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.common.collect.Table.Cell;
import com.google.gson.stream.JsonReader;

public class ReliableTaildirEventReader {
	private static final Logger logger = LoggerFactory
			.getLogger(ReliableTaildirEventReader.class);
	
	private final Table<String, File, Pattern> tailFileTable;
	// private final Table<String, String, String> headerTable;
	
	private TailFile currentFile = null;
	private Map<Long, TailFile> tailFiles = Maps.newHashMap();
	private long updateTime;
	private boolean committed = true;
	private String os = CommonUtil.getOs();
	/**
	 * Create a ReliableTaildirEventReader to watch the given directory.
	 */
	private ReliableTaildirEventReader(Map<String, String> filePaths,
			String positionFilePath, boolean skipToEnd) throws IOException {
		// Sanity checks
		Preconditions.checkNotNull(filePaths);
		Preconditions.checkNotNull(positionFilePath);

		if (logger.isDebugEnabled()) {
			logger.debug(
					"Initializing {} with directory={}, metaDir={}",
					new Object[] {
							ReliableTaildirEventReader.class.getSimpleName(),
							filePaths });
		}

		Table<String, File, Pattern> tailFileTable = HashBasedTable.create();
		for (Entry<String, String> e : filePaths.entrySet()) {
			File f = new File(e.getValue());
			File parentDir = f.getParentFile();
			try {
				Preconditions.checkState(parentDir.exists(),
						"Directory does not exist: " + parentDir.getAbsolutePath());
			} catch (IllegalStateException ex) {
				// do nothing.
			}
			Pattern fileNamePattern = Pattern.compile(f.getName());
			tailFileTable.put(e.getKey(), parentDir, fileNamePattern);
		}
		logger.info("tailFileTable: " + tailFileTable.toString());
		// logger.info("headerTable: " + headerTable.toString());

		this.tailFileTable = tailFileTable;
		// this.headerTable = headerTable;
		// this.addByteOffset = addByteOffset;
		updateTailFiles(skipToEnd);

		logger.info("Updating position from position file: " + positionFilePath);
		loadPositionFile(positionFilePath);
	}

	/**
	 * Load a position file which has the last read position of each file. If
	 * the position file exists, update tailFiles mapping.
	 */
	public void loadPositionFile(String filePath) {
		Long inode, pos;
		String path;
		FileReader fr = null;
		JsonReader jr = null;
		try {
			fr = new FileReader(filePath);
			jr = new JsonReader(fr);
			jr.beginArray();
			while (jr.hasNext()) {
				inode = null;
				pos = null;
				path = null;
				jr.beginObject();
				while (jr.hasNext()) {
					switch (jr.nextName()) {
					case "inode":
						inode = jr.nextLong();
						break;
					case "pos":
						pos = jr.nextLong();
						break;
					case "file":
						path = jr.nextString();
						break;
					}
				}
				jr.endObject();

				for (Object v : Arrays.asList(inode, pos, path)) {
					Preconditions.checkNotNull(v,
							"Detected missing value in position file. "
									+ "inode: " + inode + ", pos: " + pos
									+ ", path: " + path);
				}
				TailFile tf = tailFiles.get(inode);
				if (tf != null && tf.updatePos(path, inode, pos)) {
					tailFiles.put(inode, tf);
				} else {
					logger.info("Missing file: " + path + ", inode: " + inode
							+ ", pos: " + pos);
				}
			}
			jr.endArray();
		} catch (FileNotFoundException e) {
			logger.info("File not found: " + filePath
					+ ", not updating position");
		} catch (IOException e) {
			logger.error("Failed loading positionFile: " + filePath, e);
		} finally {
			try {
				if (fr != null)
					fr.close();
				if (jr != null)
					jr.close();
			} catch (IOException e) {
				logger.error("Error: " + e.getMessage(), e);
			}
		}
	}

	public Map<Long, TailFile> getTailFiles() {
		return tailFiles;
	}

	public void setCurrentFile(TailFile currentFile) {
		this.currentFile = currentFile;
	}

	// @Override
	public String readEvent() throws IOException {
		List<String> events = readEvents(1);
		if (events.isEmpty()) {
			return null;
		}
		return events.get(0);
	}

	// @Override
	public List<String> readEvents(int numEvents) throws IOException {
		return readEvents(numEvents, false);
	}

	@VisibleForTesting
	public List<String> readEvents(TailFile tf, int numEvents)
			throws IOException {
		setCurrentFile(tf);
		return readEvents(numEvents, true);
	}

	public List<String> readEvents(int numEvents, boolean backoffWithoutNL)
			throws IOException {
		if (!committed) {
			if (currentFile == null) {
				throw new IllegalStateException("currentFile does not exsit");
			}
			logger.info("Last read was never committed - resetting position");
			long lastPos = currentFile.getPos();
			currentFile.getRaf().seek(lastPos);
		}
		List<String> events = currentFile.readEvents(numEvents,
				backoffWithoutNL);
		if (events.isEmpty()) {
			return events;
		}

		/*
		 * Map<String, String> headers = currentFile.getHeaders(); if (headers
		 * != null && !headers.isEmpty()) { for (Event event : events) {
		 * event.getHeaders().putAll(headers); } }
		 */
		committed = false;
		return events;
	}

	// @Override
	public void close() throws IOException {
		for (TailFile tf : tailFiles.values()) {
			if (tf.getRaf() != null)
				tf.getRaf().close();
		}
	}

	/** Commit the last lines which were read. */
	// @Override
	public void commit() throws IOException {
		if (!committed && currentFile != null) {
			long pos = currentFile.getRaf().getFilePointer();
			currentFile.setPos(pos);
			currentFile.setLastUpdated(updateTime);
			committed = true;
		}
	}

	/**
	 * Update tailFiles mapping if a new file is created or appends are detected
	 * to the existing file.
	 */
	public List<Long> updateTailFiles(boolean skipToEnd) throws IOException {
		updateTime = System.currentTimeMillis();
		List<Long> updatedInodes = Lists.newArrayList();

		for (Cell<String, File, Pattern> cell : tailFileTable.cellSet()) {
			/* Map<String, String> headers = headerTable.row(cell.getRowKey()); */
			File parentDir = cell.getColumnKey();
			Pattern fileNamePattern = cell.getValue();

			for (File f : getMatchFiles(parentDir, fileNamePattern)) {
				long inode = getInode(f); // f.getName().hashCode();//getInode(f); windows返回absoluePath.hashCode().
				TailFile tf = tailFiles.get(inode);
				if (tf == null ){//新增文件, inode没变，文件名变了不影响.//|| !tf.getPath().equals(f.getAbsolutePath())) {
					long startPos = skipToEnd ? f.length() : 0;
					tf = openFile(f, inode, startPos);
				} else {
					boolean updated = tf.getLastUpdated() < f.lastModified();
					if (updated) {
						if (tf.getRaf() == null) {
							tf = openFile(f, inode, tf.getPos());
						}
					}
					long length = f.length();
					if (length < tf.getPos()) {
						logger.info("Pos " + tf.getPos()
								+ " is larger than file size! "
								+ "Restarting from pos 0, file: "
								+ tf.getPath() + ", inode: " + inode);
						tf.updatePos(tf.getPath(), inode, 0);
					} else if (length > tf.getPos()) {
						updated = true;
						if (tf.getRaf() == null) {
							tf = openFile(f, inode, tf.getPos());
						}
					}
					tf.setNeedTail(updated);
				}
				tailFiles.put(inode, tf);
				updatedInodes.add(inode);
			}
		}
		return updatedInodes;
	}

	public List<Long> updateTailFiles() throws IOException {
		return updateTailFiles(false);
	}

	private List<File> getMatchFiles(File parentDir,
			final Pattern fileNamePattern) {
		FileFilter filter = new FileFilter() {
			public boolean accept(File f) {
				String fileName = f.getName();
				if (f.isDirectory()
						|| !fileNamePattern.matcher(fileName).matches()) {
					return false;
				}
				return true;
			}
		};
		List<File> matchFiles = Lists.newArrayList();
		File[] files = parentDir.listFiles(filter);
		if (files != null){
			for (File f : Arrays.asList(files)) {
				matchFiles.add(f);
			}
		}
		return matchFiles;
	}

	private long getInode(File file) throws IOException {
		long inode = os.equalsIgnoreCase("windows") ? file.getAbsoluteFile().hashCode() : (long) Files.getAttribute(file.toPath(), "unix:ino");
		return inode;
	}

	private TailFile openFile(File file, long inode, long pos) {
		try {
			logger.info("Opening file: " + file + ", inode: " + inode
					+ ", pos: " + pos);
			return new TailFile(file, inode, pos);
		} catch (IOException e) {
			throw new RuntimeException("Failed opening file: " + file, e);
		}
	}

	/**
	 * Special builder class for ReliableTaildirEventReader
	 */
	public static class Builder {
		private Map<String, String> filePaths;
		// private Table<String, String, String> headerTable;
		private String positionFilePath;
		private boolean skipToEnd;

		// private boolean addByteOffset;

		public Builder filePaths(Map<String, String> filePaths) {
			this.filePaths = filePaths;
			return this;
		}

		/*
		 * public Builder headerTable(Table<String, String, String> headerTable)
		 * { this.headerTable = headerTable; return this; }
		 */

		public Builder positionFilePath(String positionFilePath) {
			this.positionFilePath = positionFilePath;
			return this;
		}

		public Builder skipToEnd(boolean skipToEnd) {
			this.skipToEnd = skipToEnd;
			return this;
		}

		/*
		 * public Builder addByteOffset(boolean addByteOffset) {
		 * this.addByteOffset = addByteOffset; return this; }
		 */

		public ReliableTaildirEventReader build() throws IOException {
			return new ReliableTaildirEventReader(filePaths, positionFilePath,
					skipToEnd);
		}
	}

}