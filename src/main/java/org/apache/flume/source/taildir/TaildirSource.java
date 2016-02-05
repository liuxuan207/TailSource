package org.apache.flume.source.taildir;

import static org.apache.flume.source.taildir.TaildirSourceConfigurationConstants.BATCH_SIZE;
import static org.apache.flume.source.taildir.TaildirSourceConfigurationConstants.DEFAULT_BATCH_SIZE;
import static org.apache.flume.source.taildir.TaildirSourceConfigurationConstants.DEFAULT_IDLE_TIMEOUT;
import static org.apache.flume.source.taildir.TaildirSourceConfigurationConstants.DEFAULT_POSITION_FILE;
import static org.apache.flume.source.taildir.TaildirSourceConfigurationConstants.DEFAULT_RETRY_INTERVAL;
import static org.apache.flume.source.taildir.TaildirSourceConfigurationConstants.DEFAULT_SKIP_TO_END;
import static org.apache.flume.source.taildir.TaildirSourceConfigurationConstants.DEFAULT_WRITE_POS_INTERVAL;
import static org.apache.flume.source.taildir.TaildirSourceConfigurationConstants.FILE_GROUPS;
import static org.apache.flume.source.taildir.TaildirSourceConfigurationConstants.FILE_GROUPS_PREFIX;
import static org.apache.flume.source.taildir.TaildirSourceConfigurationConstants.IDLE_MAX_TIMES;
import static org.apache.flume.source.taildir.TaildirSourceConfigurationConstants.IDLE_TIMEOUT;
import static org.apache.flume.source.taildir.TaildirSourceConfigurationConstants.POSITION_FILE;
import static org.apache.flume.source.taildir.TaildirSourceConfigurationConstants.RETRY_INTERVEL;
import static org.apache.flume.source.taildir.TaildirSourceConfigurationConstants.RUNNING_FLAG_FILE;
import static org.apache.flume.source.taildir.TaildirSourceConfigurationConstants.SKIP_TO_END;
import static org.apache.flume.source.taildir.TaildirSourceConfigurationConstants.STOP_FLAG_FILE;
import static org.apache.flume.source.taildir.TaildirSourceConfigurationConstants.WRITE_POS_INTERVAL;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.gaea.tailsource.PropertiesLoader;
import com.gaea.tailsource.common.DateUtil;
import com.gaea.tailsource.common.StringUtils;
import com.gaea.tailsource.sender.AdapterSender;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.gson.Gson;

public class TaildirSource // extends AbstractSource implements PollableSource,
// Configurable
{

	private static final Logger logger = LoggerFactory
			.getLogger(TaildirSource.class);

	private Map<String, String> filePaths;
	private int batchSize;
	private String positionFilePath;
	private boolean skipToEnd;
	private boolean byteOffsetHeader;
	private int idleTimeout;
	private int writePosInterval;
	private String stop_flag_file = "";
	private String running_flag_file;

	// private KafkaClient kafkaClient;
	private AdapterSender SENDER;
	private ReliableTaildirEventReader reader;
	private ScheduledExecutorService idleFileChecker;
	private ScheduledExecutorService positionWriter;
	private int retryInterval = 50;
	private int idleMaxTime = 72000;
	private int checkIdleInterval = 5000;
	private int writePosInitDelay = 5000;
	private List<Long> existingInodes = null;
	private List<Long> idleInodes = null;
	private boolean autoNext = false;
	private boolean shouldContinue = false;
	private String today = null;

	private ConfigContext OUT_CONFIG;
	private DateUtil dateUtil = null;

	public TaildirSource() {
		existingInodes = new CopyOnWriteArrayList<Long>();
		idleInodes = new CopyOnWriteArrayList<Long>();
		dateUtil = DateUtil.newInstance();
		today = dateUtil.nowDay();
		initConfigure();
	}

	public synchronized void start() {
		logger.info("TaildirSource source starting with directory: {}",
				filePaths);
		try {
			// kafkaClient = KafkaClient.getInstance();
			String sendType = OUT_CONFIG.getString("sendType");
			if ("http".equals(sendType) || "sdkhttp".equals(sendType)) {
				String postUrl = OUT_CONFIG.getString("to_url",
						"http://logins.gaeamobile-inc.net:8090/zeus/upload");
				if (OUT_CONFIG.getString("app") != null) {
					postUrl = postUrl + "/" + OUT_CONFIG.getString("app");
				}
				logger.info("TaildirSource send msg to {}", postUrl);
				SENDER = new AdapterSender(sendType, postUrl);
			} else if ("kafka".equals(sendType)) {
				SENDER = new AdapterSender(sendType, null);
			}

			reader = new ReliableTaildirEventReader.Builder()
					.filePaths(filePaths)
					// .headerTable(headerTable)
					.positionFilePath(positionFilePath).skipToEnd(skipToEnd)
					// .addByteOffset(byteOffsetHeader)
					.build();
		} catch (IOException e) {
			throw new RuntimeException("Error for Starting..", e);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		idleFileChecker = Executors
				.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
						.setNameFormat("idleFileChecker").build());
		idleFileChecker.scheduleWithFixedDelay(new idleFileCheckerRunnable(),
				idleTimeout, checkIdleInterval, TimeUnit.MILLISECONDS);

		positionWriter = Executors
				.newSingleThreadScheduledExecutor(new ThreadFactoryBuilder()
						.setNameFormat("positionWriter").build());
		positionWriter.scheduleWithFixedDelay(new PositionWriterRunnable(),
				writePosInitDelay, writePosInterval, TimeUnit.MILLISECONDS);
		runWithInterval();
		logger.info("TaildirSource finished: " + today);
	}

	// @Override
	public synchronized void stop() {
		try {
			ExecutorService[] services = { idleFileChecker, positionWriter };
			for (ExecutorService service : services) {
				service.shutdown();
				if (!service.awaitTermination(1, TimeUnit.SECONDS)) {
					service.shutdownNow();
				}
			}
			// write the last position
			writePosition();
			reader.close();
		} catch (InterruptedException e) {
			logger.info("Interrupted while awaiting termination", e);
		} catch (IOException e) {
			logger.error("Failed: " + e.getMessage(), e);
		}
	}

	@Override
	public String toString() {
		return String
				.format("Taildir source: { positionFile: %s, skipToEnd: %s, "
						+ "byteOffsetHeader: %s, idleTimeout: %s, writePosInterval: %s }",
						positionFilePath, skipToEnd, byteOffsetHeader,
						idleTimeout, writePosInterval);
	}

	/**
	 * 获取相关配置项from context， context 依赖 flume，这块的配置型我们自己管理.
	 * 
	 */
	// @Override
	public synchronized void initConfigure() {
		updateConfig(false);
	}

	public synchronized boolean updateConfig(boolean isStarted) {
		ConfigContext context = null;
		try {
			Properties sysPro = PropertiesLoader.loadProperties(
					System.getProperty("config"), "utf-8");
			context = new ConfigContext(sysPro);
		} catch (Exception e) {
			context = this.OUT_CONFIG;
			logger.warn(e.getMessage());
			logger.warn("reload " + System.getProperty("config")
					+ " failed, and use old configuration instead.");
		}

		logger.info("************Output config***********");
		logger.info(context.toString());
		logger.info("************************************");
		
		if (!isStarted){
			// reset day to init day specified by config.properties.
			if (context.containsKey("startDate")) {
				if (context.getString("startDate") != null
						&& ! org.apache.commons.lang.StringUtils
								.isBlank(context.getString("startDate"))) {
					if (context.getString("startDate").compareToIgnoreCase(this.today) < 0){
						this.today = context.getString("startDate").trim();
						logger.info("Reset to Init day : " + today);
					}
				}
			}
		} else {
			if (context.getBoolean("skipToToday", true)) {
				today = dateUtil.nowDay();
			} else {
				today = dateUtil.incDay(today, 1);
				if (today == null) {
					today = dateUtil.nowDay();
				}
			}
		}
		
		String fileGroups = context.getString(FILE_GROUPS);
		Preconditions.checkState(fileGroups != null, "Missing param: "
				+ FILE_GROUPS);

		filePaths = selectByKeys(context, today, fileGroups.split("\\s+"));
		Preconditions.checkState(!filePaths.isEmpty(),
				"Mapping for tailing files is empty or invalid: '"
						+ FILE_GROUPS_PREFIX + "'");
		String homePath = System.getProperty("user.home").replace('\\', '/');
		positionFilePath = StringUtils.join(
				".",
				context.getString(POSITION_FILE, homePath
						+ DEFAULT_POSITION_FILE), today);
		batchSize = context.getInteger(BATCH_SIZE, DEFAULT_BATCH_SIZE);
		skipToEnd = context.getBoolean(SKIP_TO_END, DEFAULT_SKIP_TO_END);
		idleTimeout = context.getInteger(IDLE_TIMEOUT, DEFAULT_IDLE_TIMEOUT);
		writePosInterval = context.getInteger(WRITE_POS_INTERVAL,
				DEFAULT_WRITE_POS_INTERVAL);
		idleMaxTime = context.getInteger(IDLE_MAX_TIMES, 10);
		stop_flag_file = context.getString(STOP_FLAG_FILE);
		running_flag_file = StringUtils.join(".",
				context.getString(RUNNING_FLAG_FILE), today);
		retryInterval = context.getInteger(RETRY_INTERVEL,
				DEFAULT_RETRY_INTERVAL);
		/** 是否自动继续tail下一天的数据. **/
		autoNext = context.getBoolean("autoNext", false);

		// reset.
		this.setShouldContinue(false);
		logger.info("filePaths:" + filePaths.toString());
		logger.info("positionFilePath:" + positionFilePath);
		logger.info("batchSize:" + batchSize);
		logger.info("skipToEnd:" + skipToEnd);
		logger.info("idleTimeout:" + idleTimeout);
		logger.info("writePosInterval:" + writePosInterval);
		logger.info("idleMaxTime:" + idleMaxTime);
		logger.info("stop_flag_file:" + stop_flag_file);
		logger.info("running_flag_file:" + running_flag_file);
		logger.info("retryInterval:" + retryInterval);
		logger.info("autoNext:" + autoNext);
		OUT_CONFIG = context;
		return true;
	}

	@SuppressWarnings("unused")
	private String concat(String name, String suffix) {
		return name + File.separator + suffix;
	}

	private Map<String, String> selectByKeys(ConfigContext cxt, String day,
			String[] keys) {
		Map<String, String> result = Maps.newHashMap();
		String pattern;
		for (String key : keys) {
			pattern = key + ".pattern";
			if (cxt.containsKey(pattern)) {
				String[] ps = cxt.getString(pattern).split(",");
				if (ps != null && ps.length >= 2) {
					ps[0] = ps[0].replaceAll("\\$\\{(day)\\}", day);
					ps[1] = ps[1].replaceAll("\\$\\{(day)\\}", day);
					if (ps.length == 2 || !ps[2].equalsIgnoreCase("false")){
						result.put(key, ps[0] + File.separatorChar + day
								+ File.separatorChar + ps[1]);
					}else {
						result.put(key, ps[0] + File.separatorChar + ps[1]);
					}
				}
			}
		}
		return result;
	}

	public static enum Status {
		READY, BACKOFF, CLOSE
	}

	public void runWithInterval() {
		int count4Stop = 0;
		while (!hasStopFlag()) {
			if (process() == Status.READY) {
				count4Stop = 0;
			} else if (finishedToday(++count4Stop, idleMaxTime)) {
				if (this.autoNext) {
					// 完成了today的数据tail, ready to tail the next day's data.
					// reload config : update calc_day field value.
					// this.updateConfig();
					// count4Stop = 0;
					this.setShouldContinue(true);
					logger.info("["
							+ this.getClass().getName()
							+ "] 完成了today的数据tail, ready to tail the next day's data");
				} else {
					logger.info("[" + this.getClass().getName()
							+ "] finished tail today's data and ready to exit.");
				}
				break;
			} else if (count4Stop > Integer.MAX_VALUE - 10) {
				count4Stop = 0;
			}
			try {
				TimeUnit.MILLISECONDS.sleep(retryInterval);
			} catch (InterruptedException e) {
				logger.info("Interrupted while sleeping");
			}
		}
		this.stop();
		this.deleteFlag();
		logger.info("[" + this.getClass().getName() + "] Ready to exit app...");
	}

	public void setShouldContinue(boolean bool) {
		this.shouldContinue = bool;
	}

	public boolean shouldContinue() {
		return this.shouldContinue;
	}

	private boolean hasStopFlag() {
		File f = new File(stop_flag_file);
		return f.exists();
	}

	private boolean finishedToday(int retry, int needRetry) {
		if (dateUtil.diffDay(this.today, System.currentTimeMillis()) && retry >= needRetry) {
			// 跨天，且当前检查的day的数据没有更新,且重试次数大于或等于设定的最大次数.
			return true;
		}
		return false;
	}

	private void deleteFlag() {
		File f1 = new File(stop_flag_file);
		FileUtils.deleteQuietly(f1);
		File f2 = new File(running_flag_file);
		FileUtils.deleteQuietly(f2);
	}

	// @Override
	public Status process() {
		Status status = Status.CLOSE;
		try {
			existingInodes.clear();
			existingInodes.addAll(reader.updateTailFiles());
			for (long inode : existingInodes) {
				TailFile tf = reader.getTailFiles().get(inode);
				if (tf.needTail()) {
					tailFileProcess(tf, true);
					status = Status.READY;
				}
			}
			closeTailFiles();
		} catch (Throwable t) {
			logger.error("Unable to tail files", t);
			status = Status.BACKOFF;
		}
		return status;
	}

	private void tailFileProcess(TailFile tf, boolean backoffWithoutNL)
			throws IOException, InterruptedException {
		// int processCountWithException = 0;
		while (true) {
			reader.setCurrentFile(tf);
			List<String> events = reader
					.readEvents(batchSize, backoffWithoutNL);
			if (events.isEmpty()) {
				break;
			}
			try {
				SENDER.send(events.toArray(new String[0]));
				reader.commit();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
			if (events.size() <= batchSize) {
				break;
			}
		}
	}

	private void closeTailFiles() throws IOException, InterruptedException {
		for (long inode : idleInodes) {
			TailFile tf = reader.getTailFiles().get(inode);
			if (tf.getRaf() != null) { // when file has not closed yet
				tailFileProcess(tf, false);
				tf.close();
				logger.info("Closed file: " + tf.getPath() + ", inode: "
						+ inode + ", pos: " + tf.getPos());
			}
		}
		idleInodes.clear();
	}

	/**
	 * Runnable class that checks whether there are files which should be
	 * closed.
	 */
	private class idleFileCheckerRunnable implements Runnable {
		@Override
		public void run() {
			try {
				long now = System.currentTimeMillis();
				for (TailFile tf : reader.getTailFiles().values()) {
					if (tf.getLastUpdated() + idleTimeout < now
							&& tf.getRaf() != null) {
						idleInodes.add(tf.getInode());
					}
				}
			} catch (Throwable t) {
				logger.error("Uncaught exception in IdleFileChecker thread", t);
			}
		}
	}

	/**
	 * Runnable class that writes a position file which has the last read
	 * position of each file.
	 */
	private class PositionWriterRunnable implements Runnable {
		@Override
		public void run() {
			writePosition();
		}
	}

	private void writePosition() {
		File file = new File(positionFilePath);
		FileWriter writer = null;
		try {
			writer = new FileWriter(file);
			if (!existingInodes.isEmpty()) {
				String json = toPosInfoJson();
				writer.write(json);
			}
		} catch (Throwable t) {
			logger.error("Failed writing positionFile", t);
		} finally {
			try {
				if (writer != null)
					writer.close();
			} catch (IOException e) {
				logger.error("Error: " + e.getMessage(), e);
			}
		}
	}

	private String toPosInfoJson() {
		@SuppressWarnings("rawtypes")
		List<Map> posInfos = Lists.newArrayList();
		for (Long inode : existingInodes) {
			TailFile tf = reader.getTailFiles().get(inode);
			posInfos.add(ImmutableMap.of("inode", inode, "pos", tf.getPos(),
					"file", tf.getPath()));
		}
		return new Gson().toJson(posInfos);
	}
}