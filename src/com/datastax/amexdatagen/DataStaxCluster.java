package com.datastax.amexdatagen;

import java.util.ArrayList;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.Cluster.Builder;

public class DataStaxCluster {
	private String[] nodes;
	private String keyspace;
	private Cluster cluster;
	private Session session;
	
	private StatsFutureSet futures;

	//Prepared Statements
	private PreparedStatement psWriteMerchant;
	private PreparedStatement psWriteMerchantByName;
	private PreparedStatement psWriteMerchantLookup;
	
	private PreparedStatement psGetMerchantByID;
	private PreparedStatement psGetMerchantByName;
	private PreparedStatement psLookupMerchantByName;
	//private PreparedStatement psMerchantSearch;
	
	//Statements
	private Statement sGetZips;
	private Statement sGetMerchantNames;
	
	public DataStaxCluster(String[] nodes, String keyspace){
		this.setNodes(nodes);
		this.setKeyspace(keyspace);
		
		futures = new StatsFutureSet(100, 10000, 1000);
		
		connect();
		prepare();
	}
	
	private void connect(){
		Builder builder = Cluster.builder();
		builder.addContactPoints(nodes);
		cluster = builder.build();
		cluster.getConfiguration().getSocketOptions().setReadTimeoutMillis(100000);
		cluster.getConfiguration().getQueryOptions().setConsistencyLevel(ConsistencyLevel.ONE);
		session = cluster.connect(keyspace);		
	}
	
	private void prepare(){
		psWriteMerchant = session.prepare("insert into merchant(id, address_city_name, address_line1, address_line2, country, lat_lon, merchant_name, phone, state, zip) values (?,?,?,?,?,?,?,?,?,?)");
		psWriteMerchantByName = session.prepare("insert into merchant_by_name(id, address_city_name, address_line1, address_line2, country, lat_lon, merchant_name, phone, state, zip) values (?,?,?,?,?,?,?,?,?,?)");
		psWriteMerchantLookup = session.prepare("insert into merchant_lookup(id, merchant_name) values(?,?)");
		
		psGetMerchantByID = session.prepare("select * from merchant where id=?");
		psGetMerchantByName = session.prepare("select * from merchant_by_name where merchant_name = ?");
		psLookupMerchantByName = session.prepare("select * from merchant_lookup where merchant_name = ? limit 1");
		//psMerchantSearch = session.prepare("select * from merchant where solr_query = ?");
		
		sGetZips = session.prepare("select * from zips").bind();
		sGetMerchantNames = session.prepare("select merchant_name from merchant_lookup limit 50000").bind();
	}
	
	public void writeMerchant(Merchant m){
		futures.add(session.executeAsync(psWriteMerchant.bind(m.getId(), m.getAddress_city_name(), m.getAddress_line1(), m.getAddress_line2(), m.getCountry(), m.getLat_lon(), m.getMerchant_name(), m.getPhone(), m.getState(), m.getZip())), "insert_merchant");
		futures.add(session.executeAsync(psWriteMerchantByName.bind(m.getId(), m.getAddress_city_name(), m.getAddress_line1(), m.getAddress_line2(), m.getCountry(), m.getLat_lon(), m.getMerchant_name(), m.getPhone(), m.getState(), m.getZip())), "insert_merchant_by_name");
		futures.add(session.executeAsync(psWriteMerchantLookup.bind(m.getId(), m.getMerchant_name())),  "insert_lookup");
	}
	
	public void getMerchant(int merchantID){
		futures.add(session.executeAsync(psGetMerchantByID.bind(merchantID)), "query_by_id");
	}
	
	public void getMerchant(String merchantName){
		futures.add(session.executeAsync(psGetMerchantByName.bind(merchantName)), "query_by_name");
	}
	
	public void searchMerchantName(String merchantName){
		//select * from merchant where solr_query = 'merchant_name:TRAVIS~';
		String search = "'{\"q\": \"merchant_name:" + merchantName + "~\"}'";
		String statement = "select * from merchant where solr_query = " + search;
		futures.add(session.executeAsync(statement), "search_by_name");
	}
	
	public void geoSearch(Zip z){
		//select * from merchant where solr_query = '{"q": "*:*", "fq": "{!geofilt sfield=lat_lon pt=44.48,-73.22 d=1}"}';
		String search = "'{\"q\": \"*:*\", \"fq\": \"{!geofilt sfield=lat_lon pt=" + z.getLat_lon() +" d=1}\"}'";
		String statement = "select * from merchant where solr_query = " + search;
		futures.add(session.executeAsync(statement), "search_by_geo");
	}
	
	public void lookupMerchant(String merchantName){
		long start = System.currentTimeMillis();
		ResultSet results = session.execute(psLookupMerchantByName.bind(merchantName));
		long finish = System.currentTimeMillis();
		
		//For each row found, get the merchant data from the merchant table by id. Log the extra execution time for the first query.
		for (Row row : results){
			futures.add(session.executeAsync(psGetMerchantByID.bind(row.getInt("id"))), "application_join", finish-start);
		}
	}
	
	public ArrayList<Zip> getZips(){
		ArrayList<Zip> zipcodes = new ArrayList<Zip>();
		ResultSet results = session.execute(sGetZips);
		
		for (Row row: results){
			zipcodes.add(new Zip(row));
		}
		
		return zipcodes;
	}
	
	public ArrayList<String> getMerchantNames(){
		ArrayList<String> names = new ArrayList<String>();
		ResultSet results = session.execute(sGetMerchantNames);
		
		for (Row row: results){
			names.add(row.getString("merchant_name"));
		}
		
		return names;
	}	
	
	public String[] getNodes() {
		return nodes;
	}

	public void setNodes(String[] nodes) {
		this.nodes = nodes;
	}

	public String getKeyspace() {
		return keyspace;
	}

	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}

	public Cluster getCluster() {
		return cluster;
	}

	public void setCluster(Cluster cluster) {
		this.cluster = cluster;
	}

	public Session getSession() {
		return session;
	}

	public void setSession(Session session) {
		this.session = session;
	}

}
