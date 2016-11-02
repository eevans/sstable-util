Cassandra Time-window Compaction SSTable Inspector
==================================================

Run with a directory of TWCS-compacted SSTables as the only argument.

Outputs 4 columns, filename, file size, the estimated percentage of droppable
tombstones, and the repaired-at timestamp in milliseconds since the epoch (or
zero for unrepaired tables).  Rows are further separated into time windows by
the date of the window's lower bound.

Sample:

      File                                 Size       Tombstones     Repaired at
    2015-08-27
      la-47424-big-Data.db               4.6 kB            0.00%   1477925317318
    
    2015-10-22
      la-52590-big-Data.db               1.3 kB            0.00%   1477925317318
    
    2016-01-28
      la-52588-big-Data.db                37 kB           50.56%   1477925317318
    
    2016-02-25
      la-52586-big-Data.db                 1 MB           48.54%   1477925317318
    
    2016-04-07
      la-52584-big-Data.db               1.8 MB           64.00%   1477925317318
    
    2016-07-28
      la-52578-big-Data.db               1.6 GB           78.78%   1477925317318
    
    2016-08-11
      la-52580-big-Data.db               1.6 GB           81.51%   1477925317318
    
    2016-10-20
      la-52564-big-Data.db                 2 kB           70.00%   1477925317318
      la-52576-big-Data.db               1.1 GB           75.04%   1477925317318
      la-52565-big-Data.db               2.2 kB           70.00%   1477925317318
      la-52568-big-Data.db               8.2 GB           86.89%   1477925317318
      la-52582-big-Data.db             489.9 MB           45.06%   1477925317318
      la-52572-big-Data.db               1.3 GB           87.86%   1477925317318
      la-52611-big-Data.db              27.5 MB           62.04%               0
      la-52574-big-Data.db               1.4 GB           78.14%   1477925317318
      la-52570-big-Data.db              79.6 MB           80.22%   1477925317318
      la-52606-big-Data.db              65.8 MB           61.86%               0
    

