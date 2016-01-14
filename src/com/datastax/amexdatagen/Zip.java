package com.datastax.amexdatagen;

import com.datastax.driver.core.Row;

public class Zip {
	private String zipcode;
	private String city;
	private double lat;
	private double lon;
	private String state;
	private String lat_lon;
	
	public Zip(Row row){
		this.setZipcode(row.getString("zipcode"));
		this.setCity(row.getString("city"));
		this.setLat(row.getDouble("lat"));
		this.setLon(row.getDouble("long"));
		this.setState(row.getString("state"));
		this.setLat_lon(this.getLat() + "," + this.getLon());
	}
	
	public String getLat_lon() {
		return lat_lon;
	}

	public void setLat_lon(String lat_lon) {
		this.lat_lon = lat_lon;
	}

	public String getZipcode() {
		return zipcode;
	}
	public void setZipcode(String zipcode) {
		this.zipcode = zipcode;
	}
	public String getCity() {
		return city;
	}
	public void setCity(String city) {
		this.city = city;
	}
	public double getLat() {
		return lat;
	}
	public void setLat(double lat) {
		this.lat = lat;
	}
	public double getLon() {
		return lon;
	}
	public void setLon(double lon) {
		this.lon = lon;
	}
	public String getState() {
		return state;
	}
	public void setState(String state) {
		this.state = state;
	}
	
}
