package kafka_storm_hdfs;

import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 根据文件大小、时间滚动文件
 * 文件大小未达到，时间到了也滚动。
 *
 * @author 奔波儿灞
 * @since 1.0
 */
public class FileSizeAndTimedRotationPolicy implements FileRotationPolicy {
    private static final Logger LOG = LoggerFactory.getLogger(FileSizeAndTimedRotationPolicy.class);

    public static enum SizeUnit {

        KB((long)Math.pow(2, 10)),
        MB((long)Math.pow(2, 20)),
        GB((long)Math.pow(2, 30)),
        TB((long)Math.pow(2, 40));

        private long byteCount;

        private SizeUnit(long byteCount){
            this.byteCount = byteCount;
        }

        public long getByteCount(){
            return byteCount;
        }
    }

    public static enum TimeUnit {

        SECONDS((long)1000),
        MINUTES((long)1000*60),
        HOURS((long)1000*60*60),
        DAYS((long)1000*60*60*24);

        private long milliSeconds;

        private TimeUnit(long milliSeconds){
            this.milliSeconds = milliSeconds;
        }

        public long getMilliSeconds(){
            return milliSeconds;
        }
    }

    private final long maxBytes;

    private long lastOffset = 0;
    private long currentBytesWritten = 0;

    /**
     * 滚动文件间隔，默认10分钟
     */
    private final long interval;

    /**
     * 记录开始时间撮
     */
    private long startTimestamp = System.currentTimeMillis();

    public FileSizeAndTimedRotationPolicy(float count, FileSizeAndTimedRotationPolicy.SizeUnit sizeUnit,
                                          long interval, FileSizeAndTimedRotationPolicy.TimeUnit timeUnit){
        this.maxBytes = (long)(count * sizeUnit.getByteCount());
        this.interval = interval * timeUnit.getMilliSeconds();
    }

    protected FileSizeAndTimedRotationPolicy(long maxBytes, long interval) {
        this.maxBytes = maxBytes;
        this.interval = interval;
    }

    public boolean mark(Tuple tuple, long offset) {
        long diff = offset - this.lastOffset;
        this.currentBytesWritten += diff;
        this.lastOffset = offset;
        // 是否到达大小滚动
        boolean needsSizeRotation = this.currentBytesWritten >= this.maxBytes;
        // 大小滚动未到达，看时间
        if (!needsSizeRotation) {
            // 是否到达时间滚动
            return System.currentTimeMillis() - startTimestamp >= interval;
        }
        return true;
    }


    public void reset() {
        this.currentBytesWritten = 0;
        this.lastOffset = 0;
        this.startTimestamp = System.currentTimeMillis();
    }


    public FileRotationPolicy copy() {
        return new FileSizeAndTimedRotationPolicy(this.maxBytes, interval);
    }
}
