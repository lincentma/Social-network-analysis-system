/*
 * Copyright (C) IBM Corp. 2009.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.sf.xrime.utils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.ServerSocket;
import java.net.SocketException;
import java.util.Enumeration;

/**
 * Utilities for network operations.
 */
public class NetUtils {
    /**
     * Get a local IP for remote access. Used to setup communication between job controller and Mapper/Reducer.
     * @return A local IP address.
     * @throws SocketException
     */
    static public InetAddress getLocalIP() throws SocketException {
		Enumeration<NetworkInterface> netInterfaces = NetworkInterface.getNetworkInterfaces();
		  
		InetAddress ip = null;
		InetAddress localip = null;
		
		while(netInterfaces.hasMoreElements())
		{  
			NetworkInterface netIntf=netInterfaces.nextElement();
			
			Enumeration<InetAddress> address=netIntf.getInetAddresses();
			
			while(address.hasMoreElements()){
				ip=address.nextElement();
		    
			    if( !ip.isSiteLocalAddress()  
			            && !ip.isLoopbackAddress()  
			            && ip.getHostAddress().indexOf(":")==-1)  
			    {  
			    	return ip;
			    }
			    else if(ip.isSiteLocalAddress() && !ip.isLoopbackAddress() 
			    		&& ip.getHostAddress().indexOf(":")==-1){
			        localip=ip;
			    }
			}
		}
		
		return localip;
    }
    
    /**
     * Get a free local port.  Used to setup communication between job controller and Mapper/Reducer.
     * @return the port.
     */
    static public int getFreePort() {
        ServerSocket ss;
        int freePort = 0;

        for (int i = 0; i < 10; i++) {
            freePort = (int) (10000 + Math.round(Math.random() * 10000));
            freePort = freePort % 2 == 0 ? freePort : freePort + 1;
            try {
                ss = new ServerSocket(freePort);
                freePort = ss.getLocalPort();
                ss.close();
                return freePort;
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }

        try {
            ss = new ServerSocket(0);
            freePort = ss.getLocalPort();
            ss.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        
        return freePort;
    }
}
