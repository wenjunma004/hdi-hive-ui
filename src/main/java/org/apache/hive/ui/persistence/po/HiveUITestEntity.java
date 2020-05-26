package org.apache.hive.ui.persistence.po;
import javax.persistence.*;
import java.io.Serializable;

@Entity
@Table(name = "hive_ui_test")
public class HiveUITestEntity implements Serializable {
    private static final long serialVersionUID = 7357498910374491631L;
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name="id")
    private Integer id;

    @Column(name="content")
    private String content;


    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getContent() {
        return content;
    }

    public void setContent(String content) {
        this.content = content;
    }
}
