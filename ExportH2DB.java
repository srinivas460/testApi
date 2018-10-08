package com.orbit.reporting.export;

import java.io.PrintWriter;

import java.io.StringWriter;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.orbit.reporting.domain.metaobjects.business.SqlDataSource;
import com.orbit.reporting.domain.metaobjects.business.types.DataType;
import com.orbit.reporting.dto.query.ClientQueryModel;
import com.orbit.reporting.dto.query.ClientSelection;
import com.orbit.reporting.metadata.util.FileColumn;
import com.orbit.reporting.services.OrbitQueryResult;
import com.orbit.reporting.services.QueryResultService;
import com.orbit.reporting.services.UserDataSourceManager;
import com.orbit.reporting.services.exceptions.DataSourceConnectionFailedException;
import com.orbit.reporting.services.exceptions.OrbitConnectionPoolCreationException;
import com.sun.star.bridge.oleautomation.Decimal;

@Component
public class ExportH2DB {

	private final Logger logger = LoggerFactory.getLogger(ExportDataReport.class);
	

	@Autowired
	private UserDataSourceManager udsManager;
	
	
	private DataSource dataSource;
	private SqlDataSource sDataSource;
	private static final String ObjectH2db="ORB_DR_H2DB";
	
	public boolean setUpdataSource(String tableName) {

		try {
			DB_TABLE=tableName;
			sDataSource = new SqlDataSource();
			sDataSource.setObjectKey(ObjectH2db);
			sDataSource.setDriverClassName("org.h2.Driver");
			sDataSource.setDataSourceClassName("org.h2.jdbcx.JdbcDataSource");
			sDataSource.setJdbcUrl("jdbc:h2:mem:orbit_h2db_oper_report;DB_CLOSE_ON_EXIT=FALSE");
			sDataSource.setUsername("orbit");
			sDataSource.setPassword("orbit");
			sDataSource.setDialectType("H2");

			logger.debug("started pooled datasource ");
			udsManager.startPooledDataSource(sDataSource);

			logger.debug("done pooled datasource ");
			logger.debug("Testing pooled datasource ");
			boolean isTesting = udsManager.testConnection(sDataSource);
			logger.debug("completed pooled datasource :{}", isTesting);
			
			dataSource= udsManager.getDataSource(sDataSource);
			

			return isTesting;
		} catch (OrbitConnectionPoolCreationException | DataSourceConnectionFailedException e) {
			logger.debug("OrbitConnectionPoolCreationException issue : {}", e.getMessage());
			return false;
		}catch (Exception e) {
			logger.debug("Exception : ", e.getMessage());
			return false;
		}
	}

     public DataSource getDs() {
        return dataSource;
    }
	
	private List<String> dataTypes=null;
	public List<String> getDataTypes() {
		return dataTypes;
	}

	public void setDataTypes(List<String> dataTypes) {
		this.dataTypes = dataTypes;
	}
	
	
	public List<Object> getListfromSplitBy(String groupItem, String tableName,int noofColumns) throws SQLException {

		String sql = "SELECT DISTINCT "+groupItem+ " FROM "+tableName; 
		
		List<Object> result = new ArrayList<>();
		Statement stmt;
		ResultSet rs = null;
		try {
			stmt = dataSource.getConnection().createStatement();
			rs = stmt.executeQuery(sql);
			rs.next();
			  while(rs.next()) {
					  Object obj = rs.getObject(groupItem);
				  result.add(obj);  
			  }
		} catch (SQLException e) {
			e.printStackTrace();
		}finally {
			if(rs!=null) {
				rs.close();
			}
		}
		
		return result;
	}
	
	
	

	public boolean insertRecords(ClientQueryModel clientQueryModel, QueryResultService queryResultService, String createMasterInsertQuery, List<String> dataTypes) {
		// TODO Auto-generated method stub
		OrbitQueryResult queryResult = null;
		try {
			if (queryResultService != null) {
				queryResult = queryResultService.getQueryResult(clientQueryModel);
			}

			logger.debug("Total Records before inserting : "+queryResult.getRowMap().size());
			if(queryResult.getRowMap().size()>0) {
				insertTableRecords(queryResult.getRowMap(), clientQueryModel.getSelections().size()-1, createMasterInsertQuery,dataTypes);
			}
			return true;
		} catch (Exception e) {
			logger.debug("TableModel query result issue : {}", e.getMessage());
		}
		
		return false;
	}

	
	public void insertTableRecords(List recordsOfMaps, int colcount, String h2Sql, List<String> datatypes) throws Exception {
		PreparedStatement h2Stmt = null;
		Connection mConnection=null;
		logger.info("-> insertTableRecords starts");
		try {
			mConnection = dataSource.getConnection();
			h2Stmt =mConnection.prepareStatement(h2Sql);
			int n = 0;
			long start_time = System.currentTimeMillis();
			logger.info("start_time : =>" + start_time);
			 for (int i = 0 ; i < recordsOfMaps.size() ; i++) {
		            Map<String, String> myMap = (Map<String, String>) recordsOfMaps.get(i);
		            int colPos=0;
		            for (Entry<String, String> entrySet : myMap.entrySet()) {
//		            	h2Stmt=getDataStmt(h2Stmt,entrySet.getValue());
		            	Object value = entrySet.getValue();
		            	String DateType=datatypes.get(getColPos(entrySet.getKey())-1);
		            	
		            	if ("DATE".equals(DateType) || "DATETIME".equals(DateType)) {
						if(value==null ) {
							h2Stmt.setNull(getColPos(entrySet.getKey()), Types.DATE);
						}else {
							if ("DATETIME".equals(DateType)) {
								java.sql.Timestamp sqlDate = new java.sql.Timestamp((long) (value));
								h2Stmt.setTimestamp(getColPos(entrySet.getKey()), sqlDate);
							} else {
								java.sql.Date value1 = new java.sql.Date((long) (value));
								Long time = value1 != null ? value1.getTime() : null;
								h2Stmt.setDate(getColPos(entrySet.getKey()), (new java.sql.Date(time)));
							}
						}
					} else if ("INTEGER".equals(DateType)) {
						if(value==null ) {
							h2Stmt.setNull(getColPos(entrySet.getKey()), Types.INTEGER);
						}else {
							if (value instanceof Integer)
								h2Stmt.setInt(getColPos(entrySet.getKey()), (int) value);
							else if (value instanceof Long)
								h2Stmt.setLong(getColPos(entrySet.getKey()), (long) value);
							else
								h2Stmt.setObject(getColPos(entrySet.getKey()), value);
						}
					} else if ("STRING".equals(DateType)) {
						h2Stmt.setString(getColPos(entrySet.getKey()), String.valueOf(value));
					} else if ("DECIMAL".equals(DateType)) {
						if(value==null ) {
							h2Stmt.setNull(getColPos(entrySet.getKey()), Types.DECIMAL);
						}else {
							if (value instanceof Float)
								h2Stmt.setFloat(getColPos(entrySet.getKey()), (float) value);
							else if (value instanceof Double)
								h2Stmt.setDouble(getColPos(entrySet.getKey()), (double) value);
							else
								h2Stmt.setObject(getColPos(entrySet.getKey()), value);
						}
					} else /*if(value instanceof Date || value instanceof DateTime) */ {
						if(value==null ) 
							h2Stmt.setNull(getColPos(entrySet.getKey()), Types.NULL);
						else
							h2Stmt.setObject(getColPos(entrySet.getKey()), value);

					}
		            }
		            
		            h2Stmt.addBatch();
					if (n % 1000 == 0) {
						long end_time = System.currentTimeMillis();
						logger.info(n + " count processed *** " + ((end_time - start_time) / 1000));
						h2Stmt.executeBatch();
						start_time = System.currentTimeMillis();
					}
					

					n++;
			 }

			 h2Stmt.executeBatch();
			 mConnection.commit();
			logger.debug("-> insertTableRecords Ends with n: {}", n);
		} catch (Exception e) {
			StringWriter sw = new StringWriter();
			e.printStackTrace(new PrintWriter(sw));
			String exceptionAsString = sw.toString();
			logger.error("Exception in insertTableRecords()  : {}", exceptionAsString);
			throw e;
		} finally {
			if (h2Stmt != null)
				h2Stmt.close();
			if(mConnection!=null)
				mConnection.close();
		}
	}
	

	private int getColPos(String str) {
		String[] part = str.split("(?<=\\D)(?=\\d)");
		return Integer.valueOf(part[1])+1;
	}
	
//	private static final String DB_DRIVER = "org.h2.Driver";
//	private static final String DB_CONNECTION = "jdbc:h2:mem:orbit_h2db_oper_report;DB_CLOSE_DELAY=-1";
////	private static String DB_CONNECTION = "jdbc:h2:mem:~/";
//	private static final String DB_USER = "";
//	private static final String DB_PASSWORD = "";
	private static String DB_TABLE = "";
	
	

	public static String getDB_TABLE() {
		return DB_TABLE;
	}

	public static void setDB_TABLE(String dB_TABLE) {
		DB_TABLE = dB_TABLE;
	}

	public void checkTable(String query, int noofColumns) {
		Statement s;
		Connection mConnection=null;
		int count=1;
		try {
		 mConnection=dataSource.getConnection();
	
			s = mConnection.createStatement();
		
		ResultSet rs = s.executeQuery("SELECT * FROM "+query);
		while (rs.next()) {
			logger.debug(""+rs.getObject(count));
		    if(count==noofColumns) {
		    	count=1;
		    }else {
		    	count++;
		    }
		}
		rs.close() ;
		} catch (SQLException e) {
			e.printStackTrace();
		}finally {
			if(mConnection!=null)
				try {
					mConnection.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
	}
	

	public int getCount(String query) {
		Statement s;
		int count=0;
		try {
			s = dataSource.getConnection().createStatement();
		
		ResultSet r = s.executeQuery("SELECT COUNT(*) AS rowcount FROM "+query);
		r.next();
		 count = r.getInt("rowcount") ;
		r.close() ;
		} catch (SQLException e) {
		}
		return count;
	}
	
	public void runQuery(String createQuery) throws SQLException {
		Connection mConnection=null;
		PreparedStatement createPreparedStatement = null;
		logger.info("-> Query Starts");
		logger.info("-> Query Query: " + createQuery);
		try {
			mConnection=dataSource.getConnection();
			mConnection.setAutoCommit(false);
			createPreparedStatement = mConnection.prepareStatement(createQuery);
			createPreparedStatement.executeUpdate();
			createPreparedStatement.close();
			mConnection.commit();
			logger.info("-> Query Ends");

		} catch (SQLException e) {
			StringWriter sw = new StringWriter();
			e.printStackTrace(new PrintWriter(sw));
			String exceptionAsString = sw.toString();
			logger.info("SQLException in executeH2DML() " + exceptionAsString);
		} catch (Exception e) {
			StringWriter sw = new StringWriter();
			e.printStackTrace(new PrintWriter(sw));
			String exceptionAsString = sw.toString();
			logger.info("Exception in executeH2DML() " + exceptionAsString);
		} finally {
				if(mConnection!=null)
					mConnection.close();
		}
	}

	
	public List<FileColumn> getMetaData(String tableName, ClientQueryModel clientQueryModel) {
		dataTypes = new ArrayList<>();
		List<FileColumn> columnsMetaData= new ArrayList<>();
		int colPos=0;
		for (ClientSelection clientSelection : clientQueryModel.getSelections()) {
			FileColumn addColumn = new FileColumn();
			DataType dataType = null;
			if (clientSelection.getProperties().containsKey("datatype")) {
				Object type = clientSelection.getProperties().get("datatype"); // as per UI stotdesc stands for display
																				// Name Column
				if (type instanceof DataType)
					dataType = (DataType) type;
				else
					dataType = DataType.valueOf((String) type);
			}
			dataTypes.add(((DataType)dataType).getName());
			addColumn.setColumnDataType(dataType);

			addColumn.setColumnName("COL"+colPos);
			columnsMetaData.add(addColumn);

			colPos++;
		}
		
		return columnsMetaData;
		
	}
	
	
	public String getMasterTableQuery(String tableName,ClientQueryModel clientQueryModel) {
		StringBuilder query=new StringBuilder();
		dataTypes=new ArrayList<>();

		
		query.append("CREATE TABLE "+tableName+" (");
		int noofColumns=clientQueryModel.getSelections().size()-1;
		int colPos=0;
		for(ClientSelection clientSelection : clientQueryModel.getSelections()) {
			String dataType = null,columnName="";
			if (clientSelection.getProperties().containsKey("tc"))
				columnName=	(String)clientSelection.getProperties().get("tc"); // as per UI stotdesc stands for display Name Column
			if (clientSelection.getProperties().containsKey("datatype")) {
			Object	type=clientSelection.getProperties().get("datatype"); // as per UI stotdesc stands for display Name Column
				if(type instanceof DataType) 
					dataType = ((DataType) type).getName();
				else
					dataType = (String) type;
			}
			query.append("COL"+colPos+getDatatype(dataType));
			dataTypes.add(dataType);
			if(colPos!=noofColumns) {
				query.append(",");
			}
			
			colPos++;
		}
		query.append(")");
		return query.toString();
	}
	public boolean dropTable(String tableName) {
		try {
			String query ="DROP TABLE IF EXISTS "+tableName;
			runQuery(query);
			return true;
		}catch (Exception e) {	
			return false;
		}
	}
	public String getMasterInsertQuery(String tableName, ClientQueryModel clientQueryModel) {
		// TODO Auto-generated method stub
		StringBuilder query=new StringBuilder();
		
		query.append("INSERT INTO "+tableName+"(");
		
		int noofColumns=clientQueryModel.getSelections().size()-1;
		int colPos=0;
		while(colPos<=noofColumns) {
			query.append("COL"+colPos);
			
			if(colPos!=noofColumns) {
				query.append(",");
			}
			colPos++;
		}

		colPos=0;
		query.append(") VALUES (");
		while(colPos<=noofColumns) {
			query.append("?");
			
			if(colPos!=noofColumns) {
				query.append(",");
			}
			colPos++;
		}
		query.append(")");
		return query.toString();
	}
	
	private String getDatatype(String datatype) {
		
		String result ="";
		try {
		switch (datatype) {
			case "STRING":
				result=" VARCHAR(255) ";
				break;
			case "INTEGER":
				result=" INT ";
				break;
			case "DECIMAL":
				result=" DECIMAL ";
				break;
			case "DATE":
				result=" DATE ";

				break;
			case "DATETIME":
				result=" TIMESTAMP ";

				break;
			case "BOOLEAN":
				result=" BOOLEAN ";

				break;
			case "UNKNOWN":
				result=" OTHER ";
				break;

		default:
			break;
		}
		}catch (Exception e) {
			result="VARCHAR(255)";
		}
		
		return result;
		
	}
	public  String generateUniqueTimeStamp() {
		return  "Stage_"+new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss").format(new Date());
	}

	
}
