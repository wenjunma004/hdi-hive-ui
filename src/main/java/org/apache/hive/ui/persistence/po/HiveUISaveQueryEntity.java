package org.apache.hive.ui.persistence.po;
import javax.persistence.*;
import java.io.Serializable;

@Entity
@Table(name = "hive_ui_saved_query")
public class HiveUISaveQueryEntity implements Serializable {

    private static final long serialVersionUID = -6321419304489339915L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name="id")
    private Integer id;

    @Column(name="userid")
    private Integer userID;

    @Column(name="hive_ui_dataBase", length = 3000)
    private String dataBase;

    @Column(name="queryFile", length = 3000)
    private String queryFile;

    @Column(name="shortQuery", length = 3000)
    private String shortQuery;

    @Column(name="title", length = 3000)
    private String title;

    @Column(name="owner", length = 3000)
    private String owner;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public Integer getUserID() {
        return userID;
    }

    public void setUserID(Integer userID) {
        this.userID = userID;
    }

    public String getDataBase() {
        return dataBase;
    }

    public void setDataBase(String dataBase) {
        this.dataBase = dataBase;
    }

    public String getQueryFile() {
        return queryFile;
    }

    public void setQueryFile(String queryFile) {
        this.queryFile = queryFile;
    }

    public String getShortQuery() {
        return shortQuery;
    }

    public void setShortQuery(String shortQuery) {
        this.shortQuery = shortQuery;
    }

    public String getTitle() {
        return title;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    @Override
    public String toString() {
        return "HiveUISaveQueryEntity{" +
                "id=" + id +
                ", userID=" + userID +
                ", dataBase='" + dataBase + '\'' +
                ", queryFile='" + queryFile + '\'' +
                ", shortQuery='" + shortQuery + '\'' +
                ", title='" + title + '\'' +
                ", owner='" + owner + '\'' +
                '}';
    }
}
