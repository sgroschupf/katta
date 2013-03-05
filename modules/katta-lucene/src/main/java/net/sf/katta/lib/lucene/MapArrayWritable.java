package net.sf.katta.lib.lucene;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.MapWritable;

public class MapArrayWritable extends ArrayWritable {
  public MapArrayWritable() {
    super(MapWritable.class);
  }
  
  public MapArrayWritable(MapWritable[] values) {
    super(MapWritable.class, values);
  }

}
