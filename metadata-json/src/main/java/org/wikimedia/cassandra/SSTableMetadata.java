package org.wikimedia.cassandra;

import java.io.File;
import java.io.IOException;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.metadata.CompactionMetadata;
import org.apache.cassandra.io.sstable.metadata.MetadataComponent;
import org.apache.cassandra.io.sstable.metadata.MetadataType;
import org.apache.cassandra.io.sstable.metadata.StatsMetadata;
import org.apache.cassandra.io.sstable.metadata.ValidationMetadata;

public class SSTableMetadata {
    private long fileSize;
    private String fileName;
    private String descriptor;
    private String replayPosition;
    private String partitioner;
    private double bloomFilterFPChance;
    private long minTimestamp;
    private long maxTimestamp;
    private int maxLocalDeletionTime;
    private double compressionRatio;
    private double estimatedDroppableTombstones;
    private int level;
    private long repairedAt;
    private Set<Integer> ancestors;
    private long cardinality;

    public long getDataFileSize() {
        return fileSize;
    }

    public void setDataFileSize(long fileSize) {
        this.fileSize = fileSize;
    }

    public String getDataFileName() {
        return fileName;
    }

    public void setDataFileName(String fileName) {
        this.fileName = fileName;
    }

    public String getDescriptor() {
        return descriptor;
    }

    public void setDescriptor(String descriptor) {
        this.descriptor = descriptor;
    }

    public String getReplayPosition() {
        return replayPosition;
    }

    public void setReplayPosition(String replayPosition) {
        this.replayPosition = replayPosition;
    }

    public String getPartitioner() {
        return partitioner;
    }

    public void setPartitioner(String partitioner) {
        this.partitioner = partitioner;
    }

    public double getBloomFilterFPChance() {
        return bloomFilterFPChance;
    }

    public void setBloomFilterFPChance(double bloomFilterFPChance) {
        this.bloomFilterFPChance = bloomFilterFPChance;
    }

    public long getMinTimestamp() {
        return minTimestamp;
    }

    public void setMinTimestamp(long minTimestamp) {
        this.minTimestamp = minTimestamp;
    }

    public long getMaxTimestamp() {
        return maxTimestamp;
    }

    public void setMaxTimestamp(long maxTimestamp) {
        this.maxTimestamp = maxTimestamp;
    }

    public int getMaxLocalDeletionTime() {
        return maxLocalDeletionTime;
    }

    public void setMaxLocalDeletionTime(int maxLocalDeletionTime) {
        this.maxLocalDeletionTime = maxLocalDeletionTime;
    }

    public double getCompressionRatio() {
        return compressionRatio;
    }

    public void setCompressionRatio(double compressionRatio) {
        this.compressionRatio = compressionRatio;
    }

    public double getEstimatedDroppableTombstones() {
        return estimatedDroppableTombstones;
    }

    public void setEstimatedDroppableTombstones(double estimatedDroppableTombstones) {
        this.estimatedDroppableTombstones = estimatedDroppableTombstones;
    }

    public int getLevel() {
        return level;
    }

    public void setLevel(int level) {
        this.level = level;
    }

    public long getRepairedAt() {
        return repairedAt;
    }

    public void setRepairedAt(long repairedAt) {
        this.repairedAt = repairedAt;
    }

    public Set<Integer> getAncestors() {
        return ancestors;
    }

    public void setAncestors(Set<Integer> ancestors) {
        this.ancestors = ancestors;
    }

    public long getCardinality() {
        return cardinality;
    }

    public void setCardinality(long cardinality) {
        this.cardinality = cardinality;
    }

    public static SSTableMetadata fromFile(String fname) throws IOException {
        Descriptor descriptor = Descriptor.fromFilename(fname);
        Map<MetadataType, MetadataComponent> metadata = descriptor
                .getMetadataSerializer()
                .deserialize(descriptor, EnumSet.allOf(MetadataType.class));
        ValidationMetadata validation = (ValidationMetadata) metadata.get(MetadataType.VALIDATION);
        StatsMetadata stats = (StatsMetadata) metadata.get(MetadataType.STATS);
        CompactionMetadata compaction = (CompactionMetadata) metadata.get(MetadataType.COMPACTION);

        SSTableMetadata obj = new SSTableMetadata();
        obj.setDataFileSize(new File(fname).length());
        obj.setDataFileName(new File(fname).getName());
        obj.setDescriptor(descriptor.toString());

        if (validation != null) {
            obj.setPartitioner(validation.partitioner);
            obj.setBloomFilterFPChance(validation.bloomFilterFPChance);
        }

        if (stats != null) {
            obj.setMinTimestamp(stats.minTimestamp);
            obj.setMaxTimestamp(stats.maxTimestamp);
            obj.setMaxLocalDeletionTime(stats.maxLocalDeletionTime);
            obj.setCompressionRatio(stats.compressionRatio);
            obj.setEstimatedDroppableTombstones(
                    stats.getEstimatedDroppableTombstoneRatio((int) (System.currentTimeMillis() / 1000)));
            obj.setLevel(stats.sstableLevel);
            obj.setRepairedAt(stats.repairedAt);
            obj.setReplayPosition(stats.replayPosition.toString());
            // TODO: Add estimated tombstone drop times
            // TODO: Add histograms
        }

        if (compaction != null) {
            obj.setAncestors(compaction.ancestors);
            obj.setCardinality(compaction.cardinalityEstimator.cardinality());
        }

        return obj;
    }

}
