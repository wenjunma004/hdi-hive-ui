package org.apache.hive.ui.resources.settings;
import org.apache.hive.ui.persistence.po.HiveUISettingEntity;
import org.apache.hive.ui.persistence.service.HiveUISettingService;
import java.util.ArrayList;
import java.util.List;

public class SettingsResourceManager {

    public static List<Setting> getSettings() {
        List<HiveUISettingEntity> res = HiveUISettingService.findSettingsByUserId(1);
        List<Setting> items = new ArrayList<>();
        if(res != null){
            for(HiveUISettingEntity entity: res){
                Setting setting = new Setting();
                setting.setKey(entity.getKey());
                setting.setValue(entity.getValue());
                setting.setId(entity.getId().toString());
                setting.setOwner(entity.getOwner());
                items.add(setting);
            }
        }
        return items;
    }
}
