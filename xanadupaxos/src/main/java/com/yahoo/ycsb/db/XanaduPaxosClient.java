package com.yahoo.ycsb.db;

import com.xanadu.store.*;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;

import com.xanadu.server.DistributedRegistry;
import com.xanadu.util.ConfigHandler;
import com.xanadu.util.ReferenceTools;
import com.xanadu.XanaduConnectionFactory;

import java.io.*;
import java.net.InetSocketAddress;
import java.util.*;

/**
 * Created by Colin on 4/28/2018.
 * Xanadu Paxos binding for YCSB
 */
public class XanaduPaxosClient extends DB {

  private static final String HOST_PROPERTY = "xanadu.registries";
  private static final String PORT_PROPERTY = "xanadu.port";
  private static final String REPLICA_PROPERTY = "xanadu.replicas";

  private static final DataStoreURI STORE_URI = new DataStoreURI("YCSB");
  private static final boolean USE_CHECKSUM = true;
  private static final int LOOKUP_LIMIT = 1;
  private static final int BLOCK_SIZE = 1024 * 1024;

  private KeyValueStore keyValueStore;
  private DataStore dataStore;

  @Override
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
      keyValueStore = XanaduConnectionFactory.createPaxosKeyValueStore(addresses);
      DataBlockStore blockStore = XanaduConnectionFactory.createDataBlockStore(addresses, STORE_URI, replicas, LOOKUP_LIMIT, USE_CHECKSUM);
      dataStore = XanaduConnectionFactory.createDataStore(blockStore, BLOCK_SIZE);
    } catch (IOException e) {
      System.err.println("Failed to create stores. IO exception.");
    }
  }

  @Override
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
  @Override
  public Status read(String table, String key, Set<String> fields, HashMap<String, ByteIterator> result) {
    try {
      KeyValue kv = keyValueStore.getValueBefore(key, 0, Long.MAX_VALUE);

      HashMap<String, String> stringMap = convertFromStream(dataStore.readData(kv.reference));
      result.putAll(StringByteIterator.getByteIteratorMap(stringMap));

      return Status.OK;
    } catch(IOException e) {
      System.err.println("Failed to read entry with key " + key + ". IO exception.");
      return Status.ERROR;
    } catch(NullPointerException e) {
      System.err.println("Failed to read entry with key " + key + ". Null pointer exception.");
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
  @Override
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
  @Override
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
  @Override
  public Status insert(String table, String key, HashMap<String, ByteIterator> values) {
    HashMap<String, String> stringHashMap = StringByteIterator.getStringMap(values);

    try {
      byte[] bytes = convertToBytes(stringHashMap);
      InputStream inputStream = new ByteArrayInputStream(bytes);

      long reference = dataStore.storeData(bytes.length, inputStream);
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
  @Override
  public Status delete(String table, String key) {
    return insert(table, key, new HashMap<String, ByteIterator>());
  }

  private byte[] convertToBytes(HashMap<String, String> map) throws IOException {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
         ObjectOutput out = new ObjectOutputStream(bos)) {
      out.writeObject(map);
      return bos.toByteArray();
    }
  }

  @SuppressWarnings("unchecked")
  private HashMap<String, String> convertFromStream(InputStream inputStream) throws IOException {
    try (ObjectInput in = new ObjectInputStream(inputStream)) {
      return (HashMap<String, String>) in.readObject();
    } catch (ClassNotFoundException e) {
      System.err.println("Failed to serialize hash map. ClassNotFoundException.");
    }
    return new HashMap<>();
  }
}
