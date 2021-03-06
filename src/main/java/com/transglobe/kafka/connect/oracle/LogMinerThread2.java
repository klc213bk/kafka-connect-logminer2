package com.transglobe.kafka.connect.oracle;

import static com.transglobe.kafka.connect.oracle.OracleConnectorSchema.BEFORE_DATA_ROW_FIELD;
import static com.transglobe.kafka.connect.oracle.OracleConnectorSchema.COMMITSCN_POSITION_FIELD;
import static com.transglobe.kafka.connect.oracle.OracleConnectorSchema.COMMIT_SCN_FIELD;
import static com.transglobe.kafka.connect.oracle.OracleConnectorSchema.COMMIT_TIMESTAMP_FIELD;
import static com.transglobe.kafka.connect.oracle.OracleConnectorSchema.CSF_FIELD;
import static com.transglobe.kafka.connect.oracle.OracleConnectorSchema.DATA_ROW_FIELD;
import static com.transglobe.kafka.connect.oracle.OracleConnectorSchema.DDL_TOPIC_POSTFIX;
import static com.transglobe.kafka.connect.oracle.OracleConnectorSchema.DOT;
import static com.transglobe.kafka.connect.oracle.OracleConnectorSchema.LOG_MINER_OFFSET_FIELD;
import static com.transglobe.kafka.connect.oracle.OracleConnectorSchema.OPERATION_COMMIT;
import static com.transglobe.kafka.connect.oracle.OracleConnectorSchema.OPERATION_DDL;
import static com.transglobe.kafka.connect.oracle.OracleConnectorSchema.OPERATION_DELETE;
import static com.transglobe.kafka.connect.oracle.OracleConnectorSchema.OPERATION_FIELD;
import static com.transglobe.kafka.connect.oracle.OracleConnectorSchema.OPERATION_INSERT;
import static com.transglobe.kafka.connect.oracle.OracleConnectorSchema.OPERATION_ROLLBACK;
import static com.transglobe.kafka.connect.oracle.OracleConnectorSchema.OPERATION_START;
import static com.transglobe.kafka.connect.oracle.OracleConnectorSchema.OPERATION_UPDATE;
import static com.transglobe.kafka.connect.oracle.OracleConnectorSchema.POSITION_FIELD;
import static com.transglobe.kafka.connect.oracle.OracleConnectorSchema.ROLLBACK_FIELD;
import static com.transglobe.kafka.connect.oracle.OracleConnectorSchema.ROWID_POSITION_FIELD;
import static com.transglobe.kafka.connect.oracle.OracleConnectorSchema.ROW_ID_FIELD;
import static com.transglobe.kafka.connect.oracle.OracleConnectorSchema.CONNECTOR_FIELD;
import static com.transglobe.kafka.connect.oracle.OracleConnectorSchema.SCN_FIELD;
import static com.transglobe.kafka.connect.oracle.OracleConnectorSchema.SEG_OWNER_FIELD;
import static com.transglobe.kafka.connect.oracle.OracleConnectorSchema.SQL_REDO_FIELD;
import static com.transglobe.kafka.connect.oracle.OracleConnectorSchema.TABLE_NAME_FIELD;
import static com.transglobe.kafka.connect.oracle.OracleConnectorSchema.TEMPORARY_TABLE;
import static com.transglobe.kafka.connect.oracle.OracleConnectorSchema.THREAD_FIELD;
import static com.transglobe.kafka.connect.oracle.OracleConnectorSchema.TIMESTAMP_FIELD;
import static com.transglobe.kafka.connect.oracle.OracleConnectorSchema.XID_FIELD;

import java.net.ConnectException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.transglobe.kafka.connect.oracle.models.DMLRow;
import com.transglobe.kafka.connect.oracle.models.DataSchemaStruct;
import com.transglobe.kafka.connect.oracle.models.Transaction;

public class LogMinerThread2 implements Runnable {
	static final Logger log = LoggerFactory.getLogger(LogMinerThread2.class);
	private BlockingQueue<SourceRecord> sourceRecordMq;    

	private Connection dbConn;
	private Long streamOffsetScn;
	private Long streamOffsetCtrl;
	private Long streamOffsetCommitScn;
	private String streamOffsetOperation;
	private String streamOffsetRowId;
	private String streamOffsetXid;  
	CallableStatement logMinerStartStmt;
	PreparedStatement logMinerSelect;
	String logMinerSelectSql;
	int dbFetchSize;
	ResultSet logMinerData;
	private boolean closed=false;
	Transaction transaction = null;
	LinkedHashMap<String,Transaction> trnCollection = new LinkedHashMap<>();
	boolean skipRecord=true;
	static int ix=0;
	String sqlX="";    
	int sequence = 0;
	int oldSequence = 0;
	private String topicName=null;
	private String topicConfig=null;
	private String dbNameAlias=null;
	private DataSchemaStruct dataSchemaStruct;  
	private OracleSourceConnectorUtils2 utils;
	private OracleSourceConnectorConfig2 config;

	public LogMinerThread2(BlockingQueue<SourceRecord> mq,Connection dbConn,Long streamOffsetScn,CallableStatement logMinerStartStmt,String logMinerSelectSql,int dbFetchSize,String topicConfig,String dbNameAlias,OracleSourceConnectorUtils2 utils, OracleSourceConnectorConfig2 config){
		this.sourceRecordMq = mq;
		this.dbConn = dbConn;
		this.streamOffsetScn = streamOffsetScn;
		this.logMinerStartStmt = logMinerStartStmt;
		this.logMinerSelectSql = logMinerSelectSql;
		this.dbFetchSize = dbFetchSize;
		this.topicConfig = topicConfig;
		this.dbNameAlias = dbNameAlias;
		this.utils = utils;  
		this.config = config;
	}

	@Override
	public void run() {
		try {
			while(!this.closed){
				log.info("Log miner process is waiting for 60 seconds before start on Thread");
				Thread.sleep(60000);
				skipRecord=false;
				int iError = 0;

				while(true){
					Boolean newLogFilesExists = OracleSqlUtils2.getLogFilesV2(dbConn, streamOffsetScn);
					log.info("Log miner will start , startScn : {},newLogFilesExists={} ",streamOffsetScn, newLogFilesExists);
					try {
						if (newLogFilesExists) {
							logMinerStartStmt.setLong(1, streamOffsetScn);
							logMinerStartStmt.execute();
						}
						logMinerSelect=dbConn.prepareCall(logMinerSelectSql);
						logMinerSelect.setFetchSize(dbFetchSize);
						logMinerSelect.setLong(1, streamOffsetScn);
						logMinerData=logMinerSelect.executeQuery();                     
					} catch (SQLException se) {
						iError++;
						log.error("Logminer start exception {} , {}",se.getMessage(),iError);
						if (iError>10){
							log.error("Logminer could not start successfully and it will exit");
							return;
						}                
						log.info("Waiting for log switch");
						Thread.sleep(iError*1000);
						continue;              
					}
					break;
				}
				log.info("Logminer started successfully on Thread");
				while(!this.closed && logMinerData.next()){
					try {
						if ((sequence>0)&&(logMinerData.getInt("RBASQN")-sequence)>1){
							log.error("Captured archive and log files have changed , regetting log files");
							break;
						}
						sequence = logMinerData.getInt("RBASQN");              
						String operation = logMinerData.getString(OPERATION_FIELD);
						String xid = logMinerData.getString(XID_FIELD);
						Long scn=logMinerData.getLong(SCN_FIELD);
						Timestamp timeStamp=logMinerData.getTimestamp(TIMESTAMP_FIELD);
						Timestamp commitTimeStamp=logMinerData.getTimestamp(COMMIT_TIMESTAMP_FIELD);
						Long commitScn=logMinerData.getLong(COMMIT_SCN_FIELD);
						String rowId=logMinerData.getString(ROW_ID_FIELD);
//						log.info(">>>>>" + operation+"-"+xid+"-"+scn);

						if (operation.equals(OPERATION_COMMIT)){
							transaction = trnCollection.get(xid);            
							if (transaction!=null){
								//###log.info("Commit found for xid:{}",xid);
								//transaction.setIsCompleted(true);
								if (transaction.getContainsRollback()){
									int deletedRows=0;
									List<DMLRow> dmlRowCollOrigin = transaction.getDmlRowCollection();

									LinkedList<Integer> deleteList = new LinkedList<>();
									for (int r=0;r<dmlRowCollOrigin.size();r++){
										//log.info(dmlRowCollOrigin.get(r).toString()+"-"+r);
										if (dmlRowCollOrigin.get(r).getRollback().equals("1")){
											//log.info(dmlRowCollOrigin.get(r).toString()+"RB Work RBRBRB"+" - "+r+" - "+deletedRows);
											//log.info("Will delete "+(r-deletedRows)+":"+(r-1-deletedRows));
											deleteList.add(r-deletedRows);
											deleteList.add(r-1-deletedRows);
											deletedRows+=2;
										}
									}
									for(int it : deleteList){
										//log.info("WILL DELETE "+it+"-"+dmlRowCollOrigin.get(it).getXid()+"-"+dmlRowCollOrigin.get(it).getSegName()+"-"+dmlRowCollOrigin.get(it).getRowId()+"-"+dmlRowCollOrigin.get(it).getOperation());
										dmlRowCollOrigin.remove(it);
									}

									//log.info("Setting rowCollection");
									transaction.setDmlRowCollection(dmlRowCollOrigin);
								}
								ListIterator<DMLRow> iterator = transaction.getDmlRowCollection().listIterator();
								while (iterator.hasNext()){
									//records.add(createRecords(iterator.next()));
									DMLRow row = iterator.next();                    
									row.setCommitTimestamp(commitTimeStamp);
									row.setCommitScn(commitScn);
									ix++;
									if (ix % 10000 == 0) log.info(String.format("Fetched %s rows from db:%s ",ix,dbNameAlias)+" "+sequence+" "+oldSequence+" "+row.getScn()+" "+row.getCommitScn()+" "+row.getCommitTimestamp());
									log.info(row.getScn()+"-"+row.getCommitScn()+"-"+row.getTimestamp()+"-"+"-"+row.getCommitTimestamp()+"-"+row.getXid()+"-"+row.getSegName()+"-"+row.getRowId()+"-"+row.getOperation());                    
									try {									
//										log.info(">>>>>before sourceRecordMq , now={}", new Timestamp(System.currentTimeMillis()));
										SourceRecord sourceRecord = null;
										if (config.getHeartbeatTable().equals(row.getSegName())) {			
											try {
//												conn = new OracleConnection2().connect(config);
												sourceRecord = createHeartbeatRecords(row);
												
												Struct dataStruct = dataSchemaStruct.getDataStruct();
												Timestamp heartbeatTime = (Timestamp)dataStruct.get("HEARTBEAT_TIME");
												log.info("#### connector={}, scn={}, heartbeatTime={}", config.getName(), row.getScn(), heartbeatTime);
												OracleSqlUtils2.updateLogminerReceived(dbConn, row.getScn(), heartbeatTime, config.getName());
												topicName = row.getTopic() + "." + StringUtils.upperCase(config.getName());
												
											} catch (Exception e) {
												log.error(">>>>> error msg={}, stacetrace={}", ExceptionUtils.getMessage(e), ExceptionUtils.getStackTrace(e));
											} 
										} else {
											topicName = StringUtils.upperCase(row.getTopic());
											sourceRecord = createRecords(row);
											
											// update streaming health table
											
										}
										log.info(" >>>>heartbeat table={}, segname={}", config.getHeartbeatTable(), row.getSegName());
										
										log.info(">>>>> put into sourceRecordMq");
										sourceRecordMq.offer(sourceRecord); 
//										log.info(">>>>>after sourceRecordMq, now={}", new Timestamp(System.currentTimeMillis()));
										
									} catch (Exception eCreateRecord) {                      
										log.error("Error during create record on topic {} xid :{} SQL :{}", topicName, xid,row.getSqlRedo(), eCreateRecord);
										continue;
									}                    
								}
								trnCollection.remove(xid);
							}                  
						}

						if (operation.equals(OPERATION_ROLLBACK)){
							transaction = trnCollection.get(xid);
							if (transaction!=null){
								trnCollection.remove(xid);
							}                
						}

						if (operation.equals(OPERATION_START)){
							List<DMLRow> dmlRowCollectionNew = new ArrayList<>();
							transaction = new Transaction(xid, scn, timeStamp, dmlRowCollectionNew, false);
							trnCollection.put(xid, transaction);
						}

						if ((operation.equals(OPERATION_INSERT))||(operation.equals(OPERATION_UPDATE))||(operation.equals(OPERATION_DELETE))||(operation.equals(OPERATION_DDL))){

							boolean contSF = logMinerData.getBoolean(CSF_FIELD);
							String rollback=logMinerData.getString(ROLLBACK_FIELD);
							Boolean trContainsRollback = rollback.equals("1") ? true : false;
							if (skipRecord){
								if ((scn.equals(streamOffsetCtrl))&&(commitScn.equals(streamOffsetCommitScn))&&(rowId.equals(streamOffsetRowId))&&(!contSF)){
									skipRecord=false;
								}
								log.info("Skipping data with scn :{} Commit Scn :{} Rowid :{}",scn,commitScn,rowId);
								continue;
							}                        

							String segOwner = logMinerData.getString(SEG_OWNER_FIELD); 
							String segName = logMinerData.getString(TABLE_NAME_FIELD);
							String sqlRedo = logMinerData.getString(SQL_REDO_FIELD);
							if (sqlRedo.contains(TEMPORARY_TABLE)) continue;
							if (operation.equals(OPERATION_DDL) && (logMinerData.getString("INFO").startsWith("INTERNAL DDL"))) continue;
							while(contSF){
								logMinerData.next();
								sqlRedo +=  logMinerData.getString(SQL_REDO_FIELD);
								contSF = logMinerData.getBoolean(CSF_FIELD);
							} 
							sqlX=sqlRedo;
							//@Data row = new Data(scn, segOwner, segName, sqlRedo,timeStamp,operation);
							//@topic = config.getTopic().equals("") ? (config.getDbNameAlias()+DOT+row.getSegOwner()+DOT+row.getSegName()).toUpperCase() : topic;
//							topicName = topicConfig.equals("") ? (dbNameAlias+DOT+segOwner+DOT+(operation.equals(OPERATION_DDL) ? DDL_TOPIC_POSTFIX : segName)).toUpperCase() : topicConfig;
							
							topicName = topicConfig.equals("") ? (dbNameAlias+DOT+segOwner+DOT+(operation.equals(OPERATION_DDL) ? DDL_TOPIC_POSTFIX : segName)).toUpperCase() : topicConfig;
							log.info(">>>topicName={}", topicName);
							
							DMLRow dmlRow = new DMLRow(xid, scn, commitScn , timeStamp, operation, segOwner, segName, rowId, sqlRedo,topicName,commitTimeStamp,rollback);
							//#log.info("Row :{} , scn:{} , commitScn:{} ,sqlRedo:{}",ix,scn,commitScn,sqlX);

							//dmlRowCollection2.clear();
							List<DMLRow> dmlRowCollection = new ArrayList<>();
							//###log.info("txnCollection size:{}",trnCollection.size());
							//DMLRow dmlRow = new DMLRow(xid, scn, timeStamp, operation, segOwner, segName, rowId, sqlRedo,topic);
							transaction = trnCollection.get(xid);
							if (transaction != null){                  
								dmlRowCollection = transaction.getDmlRowCollection();
								dmlRowCollection.add(dmlRow);
								transaction.setDmlRowCollection(dmlRowCollection);
								if (!transaction.getContainsRollback()) transaction.setContainsRollback(trContainsRollback);
								trnCollection.replace(xid, transaction);
							}else{
								//#log.error("Null Transaction {}",xid);                  
								dmlRowCollection.add(dmlRow);
								transaction = new Transaction(xid, scn, timeStamp, dmlRowCollection, trContainsRollback);                  
								trnCollection.put(xid, transaction);
							}
						}
						streamOffsetScn = scn;
						streamOffsetOperation = operation;
						streamOffsetCommitScn = commitScn;
						streamOffsetRowId = rowId;
						streamOffsetXid = xid;            
						oldSequence = sequence;
					} catch(Exception e){
						log.error("Inner Error during poll on topic {} SQL :{}", topicName, sqlX, e);                        
						continue;
					}
				}
				logMinerData.close();
				logMinerSelect.close();
				log.info("Logminer stopped successfully on Thread , scn:{},commitScn:{},operation:{},xid:{},rowid:{}",streamOffsetScn,streamOffsetCommitScn,streamOffsetOperation,streamOffsetXid,streamOffsetRowId);
				log.info(trnCollection.toString());
			}        
		} catch (InterruptedException ie){
			log.error("Thread interrupted exception");        
		} catch(RuntimeException re){
			log.error("Thread runtime exception");
		} catch (Exception e) {
			log.error("Thread general exception {}",e);
			try {
				OracleSqlUtils2.executeCallableStmt(dbConn, OracleConnectorSQL2.STOP_LOGMINER_CMD);  
				throw new ConnectException("Logminer stopped because of "+e.getMessage());
			} catch (Exception e2) {
				log.error("Thread general exception stop logminer {}",e2.getMessage());
			}                
		}
	}

	public void shutDown(){
		log.info("Logminer Thread shutdown called");
		this.closed=true;    
	}
	private SourceRecord createHeartbeatRecords(DMLRow dmlRow) throws Exception{
		dataSchemaStruct = utils.createDataSchema(dmlRow.getSegOwner(), dmlRow.getSegName(), dmlRow.getSqlRedo(),dmlRow.getOperation());
		if (dmlRow.getOperation().equals(OPERATION_DDL)) dmlRow.setSegName(DDL_TOPIC_POSTFIX);
		return new SourceRecord(sourcePartition(), sourceOffset(dmlRow.getScn(),dmlRow.getCommitScn(),dmlRow.getRowId()), dmlRow.getTopic(),  Schema.STRING_SCHEMA, config.getName(), dataSchemaStruct.getDmlRowSchema(), setValueV2(dmlRow,dataSchemaStruct));
	}
	private SourceRecord createRecords(DMLRow dmlRow) throws Exception{
		dataSchemaStruct = utils.createDataSchema(dmlRow.getSegOwner(), dmlRow.getSegName(), dmlRow.getSqlRedo(),dmlRow.getOperation());
		if (dmlRow.getOperation().equals(OPERATION_DDL)) dmlRow.setSegName(DDL_TOPIC_POSTFIX);
		return new SourceRecord(sourcePartition(), sourceOffset(dmlRow.getScn(),dmlRow.getCommitScn(),dmlRow.getRowId()), dmlRow.getTopic(), dataSchemaStruct.getDmlRowSchema(), setValueV2(dmlRow,dataSchemaStruct));
	}

	private Map<String,String> sourcePartition(){
		String logminerOffsetField = config.getName();
//		return Collections.singletonMap(LOG_MINER_OFFSET_FIELD, dbNameAlias);
		return Collections.singletonMap(logminerOffsetField, dbNameAlias);
	}

	private Map<String,String> sourceOffset(Long scnPosition,Long commitScnPosition,String rowId){
		//return Collections.singletonMap(POSITION_FIELD, scnPosition);
		Map<String,String> offSet = new HashMap<String,String>();
		offSet.put(POSITION_FIELD, scnPosition.toString());
		offSet.put(COMMITSCN_POSITION_FIELD, commitScnPosition.toString());
		offSet.put(ROWID_POSITION_FIELD, rowId);
		return offSet;
	}  

	private Struct setValueV2(DMLRow row,DataSchemaStruct dataSchemaStruct) {    
		Struct valueStruct = new Struct(dataSchemaStruct.getDmlRowSchema())
				.put(CONNECTOR_FIELD, config.getName())
				.put(SCN_FIELD, row.getScn())
				.put(COMMIT_SCN_FIELD, row.getCommitScn())
				.put(ROW_ID_FIELD, row.getRowId())
				.put(SEG_OWNER_FIELD, row.getSegOwner())
				.put(TABLE_NAME_FIELD, row.getSegName())
				.put(TIMESTAMP_FIELD, row.getTimestamp())
				.put(SQL_REDO_FIELD, row.getSqlRedo())
				.put(OPERATION_FIELD, row.getOperation())
				.put(DATA_ROW_FIELD, dataSchemaStruct.getDataStruct())
				.put(BEFORE_DATA_ROW_FIELD, dataSchemaStruct.getBeforeDataStruct());
		return valueStruct;

	}  

}