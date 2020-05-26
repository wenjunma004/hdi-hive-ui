package org.apache.hive.ui.persistence.po;
import javax.persistence.*;
import java.io.Serializable;

@Entity
@Table(name = "hive_ui_udf")
public class HiveUIUDFEntity implements Serializable {

    private static final long serialVersionUID = -6221419301189339915L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name="id")
    private Integer id;

    @Column(name="userid")
    private Integer userID;

    @Column(name="name", length = 3000)
    private String name;

    @Column(name="classname", length = 3000)
    private String classname;

    @Column(name="fileResource", length = 3000)
    private String fileResource;

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

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getClassname() {
        return classname;
    }

    public void setClassname(String classname) {
        this.classname = classname;
    }

    public String getFileResource() {
        return fileResource;
    }

    public void setFileResource(String fileResource) {
        this.fileResource = fileResource;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    @Override
    public String toString() {
        return "HIVEUIUDFEntity{" +
                "id=" + id +
                ", userID=" + userID +
                ", name='" + name + '\'' +
                ", classname='" + classname + '\'' +
                ", fileResource='" + fileResource + '\'' +
                ", owner='" + owner + '\'' +
                '}';
    }
}
