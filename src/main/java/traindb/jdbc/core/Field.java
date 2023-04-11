package traindb.jdbc.core;

public class Field {
  public static final int TEXT_FORMAT = 0;
  public static final int BINARY_FORMAT = 1;

  public String name;
  public final int type;
  public final int format;
  public final int size;
  public final int mod;

  public Field(String name, int type, int size, int mod, int format) {
    this.name = name;
    this.type = type;
    this.size = size;
    this.mod = mod;
    this.format = format;
  }

}
