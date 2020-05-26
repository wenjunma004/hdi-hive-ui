package org.apache.hive.ui.persistence.po;
import javax.persistence.*;
import java.io.Serializable;

@Entity
@Table(name = "hive_ui_setting")
public class HiveUISettingEntity implements Serializable {

    private static final long serialVersionUID = -6221419333489339915L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name="id")
    private Integer id;

    @Column(name="userid")
    private Integer userID;

    @Column(name="hive_ui_key", length = 3000)
    private String key;

    @Column(name="hive_ui_value", length = 3000)
    private String value;

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

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

    public String getOwner() {
        return owner;
    }

    public void setOwner(String owner) {
        this.owner = owner;
    }

    @Override
    public String toString() {
        return "HiveUISettingEntity{" +
                "id=" + id +
                ", userID=" + userID +
                ", key='" + key + '\'' +
                ", value='" + value + '\'' +
                ", owner='" + owner + '\'' +
                '}';
    }
}
