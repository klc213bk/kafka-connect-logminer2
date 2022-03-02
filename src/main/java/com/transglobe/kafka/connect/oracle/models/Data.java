package com.transglobe.kafka.connect.oracle.models;

import java.sql.Timestamp;

/**
 *  
 * @author Erdem Cer (erdemcer@gmail.com)
 */

public class Data{

    private long scn;
    private String segOwner;
    private String segName;
    private String sqlRedo;
    private Timestamp timeStamp;
    private String operation;
    private Long commitScn = 0L;
    private String rowId = null;

    public Data(Long scn,String segOwner,String segName,String sqlRedo,Timestamp timeStamp,String operation, Long commitScn, String rowId){
        super();
        this.scn=scn;
        this.segOwner=segOwner;
        this.segName=segName;
        this.sqlRedo=sqlRedo;
        this.timeStamp=timeStamp;
        this.operation=operation;
        this.commitScn = commitScn;
        this.rowId = rowId;
    }

    public long getScn(){
        return scn;
    }

    public String getSegOwner(){
        return segOwner;
    }

    public String getSegName(){
        return segName;
    }

    public String getSqlRedo(){
        return sqlRedo;
    }

    public Timestamp getTimeStamp(){
        return timeStamp;
    }

    public String getOperation(){
        return operation;
    }

    public void setScn(Long scn){
        this.scn=scn;
    }

    public void setSegOwner(String segOwner){
        this.segOwner=segOwner;
    }

    public void setSegName(String segName){
        this.segName=segName;
    }

    public void setSqlRedo(String sqlRedo){
        this.sqlRedo=sqlRedo;
    }

    public void setTimeStamp(Timestamp timeStamp){
        this.timeStamp=timeStamp;
    }

    public void setOperation(String operation){
        this.operation=operation;
    }

	public Long getCommitScn() {
		return commitScn;
	}

	public String getRowId() {
		return rowId;
	}
    
   
}