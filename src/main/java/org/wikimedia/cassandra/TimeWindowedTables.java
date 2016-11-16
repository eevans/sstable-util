package org.wikimedia.cassandra;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.utils.Pair;

import com.google.common.collect.HashMultimap;
import com.jeffjirsa.cassandra.db.compaction.TimeWindowCompactionStrategy;

public class TimeWindowedTables {

    private final HashMultimap<Long, SSTableMetadata> buckets = HashMultimap.<Long, SSTableMetadata> create();
    private final TimeUnit windowTimeUnit;
    private final int windowTimeSize;

    public TimeWindowedTables(TimeUnit windowTimeUnit, int windowTimeSize) {
        this.windowTimeUnit = windowTimeUnit;
        this.windowTimeSize = windowTimeSize;
    }

    public Map<Long, Collection<SSTableMetadata>> get() {
        return this.buckets.asMap();
    }

    public TimeWindowedTables addSSTable(File file) throws IOException {
        SSTableMetadata ssMeta = SSTableMetadata.fromFile(file.getAbsolutePath());
        // FIXME: This scaling to milliseconds hard-codes an assumption on microseconds.
        long maxTimestamp = ssMeta.getMaxTimestamp() / 1000L;
        Pair<Long, Long> bounds = TimeWindowCompactionStrategy.getWindowBoundsInMillis(this.windowTimeUnit, this.windowTimeSize, maxTimestamp);
        buckets.put(bounds.left, ssMeta);
        return this;
    }

    public TimeWindowedTables addSSTables(File[] files) throws IOException {
        for (File file : files)
            addSSTable(file);
        return this;
    }

    public static TimeWindowedTables fromSSTables(File[] files) throws IOException {
        return fromSSTables(TimeUnit.DAYS, 14, files);
    }

    public static TimeWindowedTables fromSSTables(TimeUnit windowTimeUnit, int windowTimeSize, File...files) throws IOException {
        return new TimeWindowedTables(windowTimeUnit, windowTimeSize).addSSTables(files);
    }

}
