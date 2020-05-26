package org.apache.hive.ui.persistence.po;
import javax.persistence.*;
import java.io.Serializable;

@Entity
@Table(name = "hive_ui_job")
public class HiveIJobEntity implements Serializable {

    private static final long serialVersionUID = -7050885244993988266L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name="id")
    private Integer jobID;

    @Column(name="userid")
    private Integer userID;

    @Column(name="applicationId", length = 3000)
    private String applicationId;

    @Column(name="confFile", length = 3000)
    private String confFile;

    @Column(name="dagId", length = 3000)
    private String dagId;

    @Column(name="dagName", length = 3000)
    private String dagName;

    @Column(name="hive_ui_dataBase", length = 3000)
    private String dataBase;

    @Column(name="dateSubmitted")
    private long dateSubmitted;

    @Column(name="duration")
    private long duration;

    @Column(name="forcedContent", length = 3000)
    private String forcedContent;

    @Column(name="globalSettings", length = 3000)
    private String globalSettings;

    @Column(name="guid", length = 3000)
    private String guid;

    @Column(name="hiveQueryId", length = 3000)
    private String hiveQueryId;

    @Column(name="logFile", length = 3000)
    private String logFile;

    @Column(name="owner", length = 3000)
    private String owner;

    @Column(name="queryFile", length = 3000)
    private String queryFile;

    @Column(name="queryId", length = 3000)
    private String queryId;

    @Column(name="referrer", length = 3000)
    private String referrer;

    @Column(name="sessionTag", length = 3000)
    private String sessionTag;

    @Column(name="hive_ui_sqlState", length = 3000)
    private String sqlState;

    @Column(name="status", length = 3000)
    private String status;

    @Column(name="statusDir", length = 3000)
    private String statusDir;

    @Column(name="statusMessage", length = 3000)
    private String statusMessage;

    @Column(name="title", length = 3000)
    private String title;

    public Integer getJobID() {
        return jobID;
    }

    public void setJobID(Integer jobID) {
        this.jobID = jobID;
    }

    public Integer getUserID() {
        return userID;
    }

    public void setUserID(Integer userID) {
        this.userID = userID;
    }

    public String getApplicationId() {
        return applicationId;
    }

    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    public String getConfFile() {
        return confFile;
    }

    public void setConfFile(String confFile) {
        this.confFile = confFile;
    }

    public String getDagId() {
        return dagId;
    }

    public void setDagId(String dagId) {
        this.dagId = dagId;
    }

    public String getDagName() {
        return dagName;
    }

    public void setDagName(String dagName) {
        this.dagName = dagName;
    }

    public String getDataBase() {
        return dataBase;
    }

    public void setDataBase(String dataBase) {
        this.dataBase = dataBase;
    }

    public long getDateSubmitted() {
        return dateSubmitted;
    }

    public void setDateSubmitted(long dateSubmitted) {
        this.dateSubmitted = dateSubmitted;
    }

    public long getDuration() {
        return duration;
    }

    public void setDuration(long duration) {
        this.duration = duration;
    }

    public String getForcedContent() {
        return forcedContent;
    }

    public void setForcedContent(String forcedContent) {
        this.forcedContent = forcedContent;
    }

    public String getGlobalSettings() {
        return globalSettings;
    }

    public void setGlobalSettings(String globalSettings) {
        this.globalSettings = globalSettings;
    }

    public String getGuid() {
        return guid;
    }

    public void setGuid(String guid) {
        this.guid = guid;
    }

    public String getHiveQueryId() {
        return hiveQueryId;
    }

    public void setHiveQueryId(String hiveQueryId) {
        this.hiveQueryId = hiveQueryId;
    }

    public String getLogFile() {
        return logFile;
    }

    public void setLogFile(String logFile) {
        this.logFile = logFile;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    public String getQueryFile() {
        return queryFile;
    }

    public void setQueryFile(String queryFile) {
        this.queryFile = queryFile;
    }

    public String getQueryId() {
        return queryId;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }

    public String getReferrer() {
        return referrer;
    }

    public void setReferrer(String referrer) {
        this.referrer = referrer;
    }

    public String getSessionTag() {
        return sessionTag;
    }

    public void setSessionTag(String sessionTag) {
        this.sessionTag = sessionTag;
    }

    public String getSqlState() {
        return sqlState;
    }

    public void setSqlState(String sqlState) {
        this.sqlState = sqlState;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getStatusDir() {
        return statusDir;
    }

    public void setStatusDir(String statusDir) {
        this.statusDir = statusDir;
    }

    public String getStatusMessage() {
        return statusMessage;
    }

    public void setStatusMessage(String statusMessage) {
        this.statusMessage = statusMessage;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    @Override
    public String toString() {
        return "HIVEUIJobEntity{" +
                "jobID=" + jobID +
                ", userID=" + userID +
                ", applicationId='" + applicationId + '\'' +
                ", confFile='" + confFile + '\'' +
                ", dagId='" + dagId + '\'' +
                ", dagName='" + dagName + '\'' +
                ", dataBase='" + dataBase + '\'' +
                ", dateSubmitted=" + dateSubmitted +
                ", duration=" + duration +
                ", forcedContent='" + forcedContent + '\'' +
                ", globalSettings='" + globalSettings + '\'' +
                ", guid='" + guid + '\'' +
                ", hiveQueryId='" + hiveQueryId + '\'' +
                ", logFile='" + logFile + '\'' +
                ", owner='" + owner + '\'' +
                ", queryFile='" + queryFile + '\'' +
                ", queryId='" + queryId + '\'' +
                ", referrer='" + referrer + '\'' +
                ", sessionTag='" + sessionTag + '\'' +
                ", sqlState='" + sqlState + '\'' +
                ", status='" + status + '\'' +
                ", statusDir='" + statusDir + '\'' +
                ", statusMessage='" + statusMessage + '\'' +
                ", title='" + title + '\'' +
                '}';
    }
}
