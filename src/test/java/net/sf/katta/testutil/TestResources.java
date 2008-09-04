package net.sf.katta.testutil;

import java.io.File;

public class TestResources {

  public static File INDEX1 = new File("src/test/testIndexA");
  public static File INDEX2 = new File("src/test/testIndexB");

  public static File UNZIPPED_INDEX = new File("src/test/testIndexC");
  public static File INVALID_INDEX = new File("src/test/testIndexInvalid");

  public static File SHARD1 = new File(INDEX2, "aIndex.zip");

}
