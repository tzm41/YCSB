package com.yahoo.yscb.db;

import com.xanadu.server.DistributedRegistry;
import com.xanadu.store.DataStore;
import com.xanadu.store.DataStoreURI;
import com.xanadu.store.KeyValue;
import com.xanadu.store.KeyValueStore;
import com.xanadu.util.ConfigHandler;
import com.xanadu.util.ReferenceTools;
import com.yahoo.ycsb.*;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.*;

import com.xanadu.XanaduConnectionFactory;

/**
 * Created by Colin on 5/7/17.
 * Xanadu binding for YCSB
 */
public class XanaduClient extends DB {

  private static final String HOST_PROPERTY = "xanadu.registries";
  private static final String PORT_PROPERTY = "xanadu.port";
  private static final String REPLICA_PROPERTY = "xanadu.replicas";

  private static final DataStoreURI STORE_URI = new DataStoreURI("YCSB");
  private static final boolean USE_CHECKSUM = true;
  private static final int LOOKUP_LIMIT = 1;

  private KeyValueStore keyValueStore;
  private DataStore dataStore;

  public void init() {
    Properties props = getProperties();

    int port;
    String portString = props.getProperty(PORT_PROPERTY);
    if (portString != null) {
      port = Integer.parseInt(portString);
    } else {
      port = DistributedRegistry.DEFAULT_PORT;
    }

    String registries = props.getProperty(HOST_PROPERTY);

    InetSocketAddress[] addresses = ConfigHandler.getSocketAddresses(registries, port);

    int replicas;
    String replicasString = props.getProperty(REPLICA_PROPERTY);
    if (replicasString != null) {
      replicas = Integer.parseInt(replicasString);
    } else {
      replicas = 2;
    }

    try {
      keyValueStore = XanaduConnectionFactory.createKeyValueStore(addresses);
      dataStore = XanaduConnectionFactory.createDataStore(addresses, STORE_URI, replicas, LOOKUP_LIMIT, USE_CHECKSUM);
    } catch (IOException e) {
      System.err.println("Failed to create stores. IO exception.");
    }
  }

  public void cleanup() {
    try {
      keyValueStore.close();
      dataStore.close();
    } catch (IOException e) {
      System.err.println("Failed to close stores. IO exception.");
    }
  }

  /**
   * Read a record from the database.
   * @param table The name of the table
   * @param key The record key of the record to read.
   * @param fields The list of fields to read, or null for all of them
   * @param result A HashMap of field/value pairs for the result
   * @return read result
   */
  public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    try {
      KeyValue kv = keyValueStore.getValueBefore(key, 0, Long.MAX_VALUE);

      HashMap<String, String> stringMap = convertFromBytes(dataStore.readData(kv.reference));
      result.putAll(StringByteIterator.getByteIteratorMap(stringMap));

      return Status.OK;
    } catch(IOException e) {
      System.err.println("Failed to read entry with key " + key + ". IO exception.");
      return Status.ERROR;
    }
  }

  /**
   * Scan method that is not supported by Xanadu.
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
   * Update entry of the key. Same operation as insert.
   * @param table The name of the table
   * @param key The record key of the record to write.
   * @param values A HashMap of field/value pairs to update in the record
   * @return update status
   */
  public Status update(String table, String key, HashMap<String, ByteIterator> values) {
    return insert(table, key, values);
  }

  /**
   * Insert entry of the key.
   * @param table The name of the table
   * @param key The record key of the record to insert.
   * @param values A HashMap of field/value pairs to insert in the record
   * @return insert status
   */
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    HashMap<String, String> stringHashMap = StringByteIterator.getStringMap(values);

    try {
      long reference = dataStore.storeData(convertToBytes(stringHashMap));
      keyValueStore.setValue(key, ReferenceTools.nowNanos(), STORE_URI, reference);

      return Status.OK;
    } catch (IOException e) {
      System.err.println("Failed to update entry with key " + key + ". IO exception.");
      return Status.ERROR;
    }
  }

  /**
   * Delete a record from the database. Same as inserting empty values.
   * @param table The name of the table
   * @param key The record key of the record to delete.
   * @return The result of the operation.
   */
  public Status delete(String table, String key) {
    return insert(table, key, new HashMap<>());
  }

  private byte[] convertToBytes(HashMap<String, String> map) throws IOException {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
         ObjectOutput out = new ObjectOutputStream(bos)) {
      out.writeObject(map);
      return bos.toByteArray();
    }
  }

  @SuppressWarnings("unchecked")
  private HashMap<String, String> convertFromBytes(byte[] bytes) throws IOException {
    try (ByteArrayInputStream bis = new ByteArrayInputStream(bytes);
         ObjectInput in = new ObjectInputStream(bis)) {
      return (HashMap<String, String>) in.readObject();
    } catch (ClassNotFoundException e) {
      System.err.println("Failed to serialize hash map. ClassNotFoundException.");
    }
    return new HashMap<>();
  }
}
