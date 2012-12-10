package com.oscargreat.cloud.stormhm.wrapper;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.log4j.Logger;

import com.oscargreat.cloud.topology.MQListenerSpout;

/***
 * Wrapper class for host Info fetch
 * 
 * @author Oscar
 * 
 */
public class MetricsQueryClient {

	HttpClient httpclient;
	public static Logger LOG = Logger.getLogger(MQListenerSpout.class);
	
	public MetricsQueryClient() {
		httpclient = new DefaultHttpClient();
	}

	public String fetchInfo(String URL){

		try{
		HttpGet httpgets = new HttpGet(URL);     
        HttpResponse response = httpclient.execute(httpgets);     
        HttpEntity entity = response.getEntity();     
        if (entity != null) {     
            InputStream instreams = entity.getContent();
            String str = streamToString(instreams);   
            httpgets.abort();
            return str;
        }   
		}catch (Exception e) {
			LOG.warn("Fail in reading host:" + URL);
		}
        return null;
	}
	
    public static String streamToString(InputStream is) {       
        BufferedReader reader = new BufferedReader(new InputStreamReader(is));       
        StringBuilder sb = new StringBuilder();       
        
        String line = null;       
        try {       
            while ((line = reader.readLine()) != null) {   
                sb.append(line + "\n");       
            }       
        } catch (IOException e) {       
            e.printStackTrace();       
        } finally {       
            try {       
                is.close();       
            } catch (IOException e) {       
               e.printStackTrace();       
            }       
        }       
        return sb.toString();       
    }   
}
