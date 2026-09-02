package cn.edu.whu.spatialJoin.common;

import java.util.GregorianCalendar;
import java.util.SimpleTimeZone;
import java.util.TimeZone;

public class DBFConstants {
  public static final byte TypeString = 'C';
  public static final byte TypeBlockNumber = 'B';
  public static final byte TypeNumeric = 'N';
  public static final byte TypeFloat = 'F';
  public static final byte TypeDouble = '0';
  public static final byte TypeBoolean = 'L';
  public static final byte TypeDate = 'D';
  public static final byte TypeDatetime = '@';
  /**Number of milliseconds in one day*/
  public static final long MillisInOneDay = 1000L * 60 * 60 * 24;
  /**UTC time zone*/
  public static final TimeZone UTC =new SimpleTimeZone(0, "UTC");
  /**The reference date for DBF dates*/
  public static final GregorianCalendar DBFEpoch;
  static {
    DBFEpoch = new GregorianCalendar(UTC);
    DBFEpoch.clear();
    DBFEpoch.set(-4713, 0, 1);
  }
}
