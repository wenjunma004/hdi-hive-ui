package org.apache.hive.ui.persistence.po;
import javax.persistence.*;
import java.io.Serializable;

@Entity
@Table(name = "hive_ui_user")
public class HiveUIUserEntity implements Serializable {

    private static final long serialVersionUID = -6221419301189339913L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name="id")
    private Integer userID;

    @Column(name="username")
    private String userName;

    public Integer getUserID() {
        return userID;
    }

    public void setUserID(Integer userID) {
        this.userID = userID;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    @Override
    public String toString() {
        return "HiveUIUserEntity{" +
                "userID=" + userID +
                ", userName='" + userName + '\'' +
                '}';
    }
}
