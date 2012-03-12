/**
 * Copyright (c) 2011 Yahoo! Inc. All rights reserved. 
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License. See accompanying LICENSE file.
 */

package com.yahoo.omid.tso;

/**
 * Holds the configuration parameters of a TSO server instance. 
 *
 */
public class TSOServerConfig {
    
    private static TSOServerConfig config;
    
    static public TSOServerConfig configFactory(){
        if(config == null){
            config = new TSOServerConfig();
        }
        
        return config;
    }
    
    static public TSOServerConfig configFactory(int port, int batch, boolean recoveryEnabled, int ensSize, int qSize, String zkservers){
        if(config == null){
            config = new TSOServerConfig(port, batch, recoveryEnabled, ensSize, qSize, zkservers);
        }
        
        return config;
    }
    
    private int port;
    private int batch;
    private boolean recoveryEnabled;
    private String zkServers;
    private int ensemble;
    private int quorum;
    
    TSOServerConfig(){
        this.port = Integer.parseInt(System.getProperty("PORT", "1234"));
        this.batch = Integer.parseInt(System.getProperty("batch", "0"));
        this.recoveryEnabled = Boolean.parseBoolean(System.getProperty("RECOVERABLE", "false"));
        this.zkServers = System.getProperty("ZKSERVERS");
        this.ensemble = Integer.parseInt(System.getProperty("ENSEMBLE", "3"));
        this.quorum = Integer.parseInt(System.getProperty("QUORUM", "2"));
    }
    
    TSOServerConfig(int port, int batch, boolean recoveryEnabled, int ensemble, int quorum, String zkServers){
        this.port = port;
        this.batch = batch;
        this.recoveryEnabled = Boolean.parseBoolean(System.getProperty("RECOVERABLE", "false"));
        this.zkServers = System.getProperty("ZKSERVERS");
        this.ensemble = Integer.parseInt(System.getProperty("ENSEMBLE", "3"));
        this.quorum = Integer.parseInt(System.getProperty("QUORUM", "2"));
    }
    
    public int getPort(){
        return port;
    }
    
    public int getBatchSize(){
        return batch;
    }
    
    public boolean isRecoveryEnabled(){
        return recoveryEnabled;
    }
    
    public String getZkServers(){
        return zkServers;
    }

    public int getEnsembleSize(){
        return ensemble;
    }
    
    public int getQuorumSize(){
        return quorum;
    }
}
