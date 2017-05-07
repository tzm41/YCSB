package com.yahoo.yscb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Status;

import java.util.HashMap;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import com.xanadu.XanaduConnectionFactory;

/**
 * Created by Colin on 5/7/17.
 * Xanadu binding for YCSB
 */
public class XanaduClient extends DB {

  public void init() {
    Properties props = getProperties();


  }

  /**
   * Scan method that is not supported by Xanadu
   * @param table The name of the table
   * @param startkey The record key of the first record to read.
   * @param recordcount The number of records to read
   * @param fields The list of fields to read, or null for all of them
   * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
   * @return Error status if this is trying to run
   */
  public Status scan(String table, String startkey, int recordcount, Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return Status.ERROR;
  }

  /**
   * Update entry of the key
   * @param table The name of the table
   * @param key The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record
   * @return update status
   */
  public Status update(String table, String key, HashMap<String, ByteIterator> values) {

  }
}
