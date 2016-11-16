package org.wikimedia.cassandra;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.PrintStream;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.config.Config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.Sets;

public class TwcsInspector {

    private static final DateFormat dataFormatter;
    private static final ObjectMapper mapper = new ObjectMapper();

    static {
        dataFormatter = new SimpleDateFormat("yyyy-MM-dd");
        dataFormatter.setTimeZone(TimeZone.getTimeZone("UTC"));

        mapper.enable(SerializationFeature.INDENT_OUTPUT);
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

        Config.setClientMode(true);

        TimeWindowedTables tables = TimeWindowedTables.fromSSTables(TimeUnit.DAYS, 14, getDataFiles(root));

        // Output results as JSON.
        if ("JSON".equals(System.getenv().getOrDefault("FORMAT", "").toUpperCase())) {
            outln(mapper.writeValueAsString(tables.get()));
        }
        // Output results human readable.
        else {
            outf("  %-32s %8s %16s %15s%n", "File", "Size", "Tombstones", "Repaired at");

            Map<Long, Collection<SSTableMetadata>> tablesMap = tables.get();
            long totalSize = 0;

            for (Long bucket : Sets.newTreeSet(tablesMap.keySet())) {
                outln(dataFormatter.format(new Date(bucket)));
                for (SSTableMetadata meta : tablesMap.get(bucket)) {
                    totalSize += meta.getDataFileSize();
                    outf(
                            "  %-32s %8s %15.2f%% %15d%n",
                            meta.getDataFileName(),
                            readableFileSize(meta.getDataFileSize()),
                            meta.getEstimatedDroppableTombstones() * 100,
                            meta.getRepairedAt());
                }
                outln();
            }

            outf("%nTotal size: %s (data files only)%n", readableFileSize(totalSize));
        }

        System.exit(0);
    }
}
