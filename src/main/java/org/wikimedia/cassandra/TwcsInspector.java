package org.wikimedia.cassandra;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintStream;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.EnumSet;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.tools.Util;
import org.apache.cassandra.utils.Pair;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.jeffjirsa.cassandra.db.compaction.TimeWindowCompactionStrategy;

public class TwcsInspector {

    private static final DateFormat dataFormatter;

    static {
        dataFormatter = new SimpleDateFormat("yyyy-MM-dd");
        dataFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    /* Copy-pasta from: http://stackoverflow.com/questions/3263892/format-file-size-as-mb-gb-etc */
    private static String readableFileSize(long size) {
        if(size <= 0) return "0";
        final String[] units = new String[] { "B", "kB", "MB", "GB", "TB" };
        int digitGroups = (int) (Math.log10(size)/Math.log10(1024));
        return new DecimalFormat("#,##0.#").format(size/Math.pow(1024, digitGroups)) + " " + units[digitGroups];
    }

    private static File[] getDataFiles(File root) {
        return root.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.contains("-Data.db") && !name.contains("tmp");
            }
        });
    }

    private static void outln() {
        outln("");
    }

    private static void outln(String val) {
        outln(System.out, val);
    }

    private static void outln(PrintStream stream, String val) {
        stream.println(val);
    }

    private static void outf(String format, Object... args) {
        outf(System.out, format, args);
    }

    private static void outf(PrintStream stream, String format, Object... args) {
        stream.printf(format, args);
    }

    private static void usage(PrintStream out) {
        out.printf("Usage: java %s <directory>%n", TwcsInspector.class.getSimpleName());
    }

    public static void main(String... args) throws IOException {
        if (args.length != 1) {
            usage(System.err);
            System.exit(1);
        }

        File root = new File(args[0]);

        if (!root.isDirectory()) {
            System.err.printf("No such directory: %s%n", root.getAbsolutePath());
            usage(System.err);
            System.exit(1);
        }

        Util.initDatabaseDescriptor();

        HashMultimap<Long, File> buckets = HashMultimap.<Long, File>create();
        Map<File, StatsMetadata> statsMap = Maps.newHashMap();

        for (File f : getDataFiles(root)) {
            Descriptor descriptor = Descriptor.fromFilename(f.getAbsolutePath());
            Map<MetadataType, MetadataComponent> metadata = descriptor.getMetadataSerializer().deserialize(descriptor, EnumSet.allOf(MetadataType.class));
            StatsMetadata stats = (StatsMetadata) metadata.get(MetadataType.STATS);
            if (stats == null) {
                System.err.printf("Warning: Skipping %s; No StatsMetadata object!");
                continue;
            }
            statsMap.put(f, stats);
            // FIXME: This scaling to milliseconds hard-codes an assumption on microseconds.
            long maxTimestamp = stats.maxTimestamp / 1000L;
            // FIXME: This hard-codes time-window size and units
            Pair<Long, Long> bounds = TimeWindowCompactionStrategy.getWindowBoundsInMillis(TimeUnit.DAYS, 14, maxTimestamp);

            buckets.put(bounds.left, f);
        }

        outf("  %-32s %8s %16s %15s%n", "File", "Size", "Tombstones", "Repaired at");

        long totalSize = 0;

        for (Long lower : Sets.newTreeSet(buckets.keySet())) {
            outln(dataFormatter.format(new Date(lower)));
            for (File f : buckets.get(lower)) {
                long size = f.length();
                totalSize += size;
                StatsMetadata stats = statsMap.get(f);
                double tombstoneRatio = stats.getEstimatedDroppableTombstoneRatio((int) (System.currentTimeMillis() / 1000));
                outf(
                        "  %-32s %8s %15.2f%% %15d%n",
                        f.getName(),
                        readableFileSize(size),
                        tombstoneRatio * 100,
                        stats.repairedAt);
            }
            outln();
        }

        outf("%nTotal size: %s (data files only)%n", readableFileSize(totalSize));

    }

}
