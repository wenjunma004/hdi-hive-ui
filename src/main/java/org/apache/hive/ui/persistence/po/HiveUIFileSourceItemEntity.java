package org.apache.hive.ui.persistence.po;
import javax.persistence.*;
import java.io.Serializable;

@Entity
@Table(name = "hive_ui_file_source_item")
public class HiveUIFileSourceItemEntity implements Serializable {

    private static final long serialVersionUID = -6221419304489339915L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name="id")
    private Integer id;

    @Column(name="userid")
    private Integer userID;

    @Column(name="name", length = 3000)
    private String name;

    @Column(name="path", length = 3000)
    private String path;

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

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    @Override
    public String toString() {
        return "HiveUIFileSourceItemEntity{" +
                "id=" + id +
                ", userID=" + userID +
                ", name='" + name + '\'' +
                ", path='" + path + '\'' +
                ", owner='" + owner + '\'' +
                '}';
    }
}
