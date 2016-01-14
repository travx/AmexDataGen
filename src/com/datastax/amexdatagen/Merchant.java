package com.datastax.amexdatagen;

public class Merchant {
	private int id;
	private String address_city_name;
	private String address_line1;
	private String address_line2;
	private String country;
	private String lat_lon; 
	private String merchant_name;
	private String phone;
	private String state;
	private String zip;

	public Merchant(
			int id,
			String address_city_name,
			String address_line1,
			String address_line2,
			String country,
			String lat_lon, 
			String merchant_name,
			String phone,
			String state,
			String zip){
		
		this.setId(id);
		this.setAddress_city_name(address_city_name);
		this.setAddress_line1(address_line1);
		this.setAddress_line2(address_line2);
		this.setCountry(country);
		this.setLat_lon(lat_lon);
		this.setMerchant_name(merchant_name);
		this.setPhone(phone);
		this.setState(state);
		this.setZip(zip);
	}
	
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getAddress_city_name() {
		return address_city_name;
	}
	public void setAddress_city_name(String address_city_name) {
		this.address_city_name = address_city_name;
	}
	public String getAddress_line1() {
		return address_line1;
	}
	public void setAddress_line1(String address_line1) {
		this.address_line1 = address_line1;
	}
	public String getAddress_line2() {
		return address_line2;
	}
	public void setAddress_line2(String address_line2) {
		this.address_line2 = address_line2;
	}
	public String getCountry() {
		return country;
	}
	public void setCountry(String country) {
		this.country = country;
	}
	public String getLat_lon() {
		return lat_lon;
	}

	public void setLat_lon(String lat_lon) {
		this.lat_lon = lat_lon;
	}	
	public String getMerchant_name() {
		return merchant_name;
	}
	public void setMerchant_name(String merchant_name) {
		this.merchant_name = merchant_name;
	}
	public String getPhone() {
		return phone;
	}
	public void setPhone(String phone) {
		this.phone = phone;
	}
	public String getState() {
		return state;
	}
	public void setState(String state) {
		this.state = state;
	}
	public String getZip() {
		return zip;
	}
	public void setZip(String zip) {
		this.zip = zip;
	}
	
	
}
