package com.datastax.amexdatagen;

import java.util.ArrayList;
import java.util.Random;

public class GenerateData {
	
	//Arguments - for executing multiple times without overwriting records
	private static int NUMRECORDS = 1000000;
	private static int OFFSET = 1000000;
	private static double READ_PCT = 0;   //Percentage of reads. A value of .7 means 70% reads. Defaults to insert-only. 1 means read-only.
	private static int MINUTES = 0;

	public static void main(String[] args) throws InterruptedException {
		// Cluster Connection
		DataStaxCluster dse = new DataStaxCluster(new String[]{"node0", "node1", "node2"}, "amexpoc");
		
		if (args.length > 0){
			NUMRECORDS = Integer.parseInt(args[0]);
		}
		
		if (args.length > 1){
			OFFSET = Integer.parseInt(args[1]);
		}
		
		if (args.length > 2){
			READ_PCT = Double.parseDouble(args[2]);
		}
		
		if (args.length > 3){
			MINUTES = Integer.parseInt(args[3]);
		}

		//List of possible zipcodes
		ArrayList<Zip> zips = dse.getZips();
		
		//List of possible merchants for name lookup
		ArrayList<String> names = dse.getMerchantNames();
		
		//Data generation functions
		DataGenerator data = new DataGenerator();
		
		//Random generator (for getting a random zip code)
		Random r = new Random();
		
		long endTime = System.currentTimeMillis() + (MINUTES * 60000);
		
		//Merchant ID
		int merchantID = OFFSET;
		
		for (int i=0; i<NUMRECORDS || System.currentTimeMillis() < endTime; i++){
			//get random city, state, zip, lat_lon
			Zip z = zips.get(r.nextInt(zips.size()));
			
			if (r.nextFloat() < READ_PCT){
				if (r.nextBoolean()){
					//Read data by merchant ID
					dse.getMerchant(r.nextInt(merchantID));
					//Read data by merchant name
					dse.getMerchant(names.get(r.nextInt(names.size())));
					//Read data by partial name match
					dse.searchMerchantName(data.getLastName());
				}
				else {
					//Read data by looking up merchant name - performs an additional read
					dse.lookupMerchant(names.get(r.nextInt(names.size())));
					//Read data by geospatial search
					dse.geoSearch(z);
				}

			}
			else{
				//Write data
				//Create merchant object with randomly generated name and address
				Merchant m = new Merchant(
						merchantID,
						z.getCity(),
						data.getAddressLine1(),
						data.getAddressLine2(),
						data.getCountry(),
						z.getLat_lon(), 
						data.getMerchantName(),
						data.getPhoneNumber(),
						z.getState(),
						z.getZipcode());
				
				//Save random data to cluster (in three tables)
				dse.writeMerchant(m);
				
				//Increment merchant id
				merchantID++;
			}
			
			//Debug
			//System.out.println(i);
			
			//Wait
			//Thread.sleep(10);
			
		}
		
		//finish
		System.out.println("Test complete.");
		System.out.println("Next Merchant ID: " + merchantID); //output the last merchantID for offset of next run
		dse.printStats();
		System.exit(0);
		
	}

}
