
# 建立資料夾路徑
mkdir -p /root/datapool/sh/nix/
mkdir -p /root/datapool/export_file/nix/
mkdir -p /root/datapool/error_log/nix/


# 更新機器人的資訊
sh /root/datapool/sh/nix/bots_lifespan/bots_detail.sh

# 計算各機器人的 interaction & lifespan
sh /root/datapool/sh/nix/bots_lifespan/bots_interaction.sh
