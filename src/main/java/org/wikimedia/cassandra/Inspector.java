package org.wikimedia.cassandra;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;

import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.tools.Util;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Inspector {

    private static Logger LOG = LoggerFactory.getLogger(Inspector.class);

    private static File[] getDataFiles(File root) {
        return root.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.contains("-Data.db") && !name.contains("tmp");
            }
        });
    }

    public static void main(String... args) throws IOException {
        File root = new File(args[0]);

        if (!root.isDirectory()) {
            System.err.printf("No such directory: %s%n", root.getAbsolutePath());
            System.exit(1);
        }

        Util.initDatabaseDescriptor();

        for (File f : getDataFiles(root)) {
            LOG.debug("Processing {}", f.getAbsolutePath());
            Descriptor descriptor = Descriptor.fromFilename(f.getAbsolutePath());
            Map<MetadataType, MetadataComponent> metadata = descriptor.getMetadataSerializer().deserialize(descriptor, EnumSet.allOf(MetadataType.class));
            StatsMetadata stats = (StatsMetadata) metadata.get(MetadataType.STATS);
            if (stats != null) {
                LOG.info("{}: maxTimestamp={}", stats.maxTimestamp);
            }
        }
    }

}
