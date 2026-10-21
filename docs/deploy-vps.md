# Deploy lên VPS để chạy 24/7

Mục tiêu: tắt PC vẫn có Airflow + pipeline chạy, UI vẫn truy cập được.

## 1. Chọn VPS

Cấu hình tối thiểu nên dùng:

| Thành phần | Khuyến nghị |
|---|---|
| CPU | 2 vCPU x86_64 (image Airflow chính thức là amd64) |
| RAM | **4 GB** (2 GB quá sát, Airflow + Postgres + ingest dễ OOM) |
| Disk | 40–80 GB SSD |
| OS | Ubuntu 24.04 hoặc 26.04 LTS |
| Băng thông | 1 TB/tháng là quá đủ (dữ liệu ~30–150 MB/ngày) |

Tham khảo (giá thay đổi theo thời điểm):

- **Hetzner CX22** (2 vCPU, 4 GB, 40 GB) — rẻ nhất, datacenter EU/US.
- **DigitalOcean Basic** (2 vCPU, 4 GB, 80 GB) — dễ dùng, có UI.
- **Vultr / Linode** — tương đương.
- **Oracle Cloud Always Free** — miễn phí nhưng hay hết slot; nếu dùng phải chọn
  instance **x86_64**, không chọn ARM để tránh lỗi image Airflow.

Nên chọn datacenter gần Việt Nam (Singapore) để UI nhanh.

## 2. Chuẩn bị VPS

Tạo VPS với **SSH key** (không dùng mật khẩu root), mở firewall chỉ port 22:

```bash
ssh root@<IP>
apt update && apt upgrade -y
```

Trên máy local, thêm SSH config cho tiện:

```
Host bigdata
    HostName <IP>
    User root
    IdentityFile ~/.ssh/id_ed25519
```

## 3. Migrate project + dữ liệu

Trên máy local:

```bash
cd "/home/hai-huynh/bigdata project"

# nên dừng stack local trước khi copy dữ liệu để đảm bảo nhất quán
make down

./scripts/migrate-to-vps.sh root@<IP> --with-data
```

Script sẽ:

1. `rsync` code (bỏ `.venv`, `logs`, `.env`) lên `~/bigdata-project`.
2. Copy `data/lake` + `data/_state` nếu có `--with-data`.
3. Cài Docker trên VPS (script `install-docker.sh`).
4. `chmod 777 data` + `docker compose up -d --build`.

Nếu chưa muốn copy dữ liệu cũ, bỏ `--with-data` — VPS sẽ tự ingest lại.

## 4. Kiểm tra trên VPS

```bash
ssh bigdata
cd ~/bigdata-project
sudo docker compose ps
curl -s localhost:8080/health
sudo docker compose exec airflow-scheduler airflow dags list
```

Bật DAG (nếu đang paused):

```bash
sudo docker compose exec airflow-scheduler airflow dags unpause crypto_market_ingest
```

Đổi mật khẩu admin và tạo viewer:

```bash
sudo docker compose exec airflow-scheduler airflow users reset-password -u admin
sudo docker compose exec airflow-scheduler airflow users create \
  --username viewer --firstname Viewer --lastname Guest \
  --role Viewer --email viewer@example.com --password 'MatKhauManh'
```

## 5. Đưa UI ra ngoài

### Cách nhanh: Cloudflare quick tunnel

```bash
curl -fsSL -o ~/.local/bin/cloudflared \
  https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64
chmod +x ~/.local/bin/cloudflared
cd ~/bigdata-project
make share
```

URL in ra dạng `https://xxx.trycloudflare.com`. Lưu ý URL đổi sau mỗi lần restart.

### Cách ổn định: named tunnel + domain

Làm theo `docs/sharing.md` mục 3. Trên VPS, sau khi có domain:

```bash
sudo cloudflared service install
sudo systemctl enable --now cloudflared
```

Nên bật Cloudflare Access để chỉ email được phép mới vào được UI.

### Tự chạy lại sau reboot

- Docker service đã `enable` nên tự start khi VPS boot.
- Container có `restart: unless-stopped` nên Airflow/Postgres tự lên lại.
- Named tunnel: `sudo systemctl enable --now cloudflared`.
- Quick tunnel **không** tự start; nếu cần tự động, dùng named tunnel hoặc tạo
  systemd unit cho `cloudflared tunnel --url ...` (URL vẫn đổi sau mỗi restart).

### Bảo mật

- Trên VPS, đặt `AIRFLOW_WEB_BIND=127.0.0.1` trong `.env` nếu **chỉ** truy cập qua
  tunnel, tránh mở port 8080 ra internet:

  ```env
  AIRFLOW_WEB_BIND=127.0.0.1
  ```

  rồi `sudo docker compose up -d`.
- `sudo ufw allow 22/tcp` và **không** mở 8080 nếu dùng tunnel.
- Không dùng `admin/admin`, không share tài khoản admin.

## 6. Sau khi migrate

- Dừng stack trên PC: `cd "/home/hai-huynh/bigdata project" && make down`.
- Dừng tunnel local: `make unshare`.
- Có thể xoá Docker/PC resource nếu không dùng nữa.
- Trỏ domain/tunnel về VPS và cập nhật link share cho người nhận.

## 7. Dữ liệu thiếu khi máy downtime

Pipeline có `CRYPTO_LOOKBACK_HOURS=24`: mỗi lần chạy sẽ quét lại 24 giờ gần nhất,
hour nào chưa có marker sẽ được tải bù. Nhờ vậy sau khi bật lại máy, tối đa 24 giờ
downtime được tự heal.

Downtime dài hơn thì backfill thủ công trên VPS:

```bash
sudo docker compose exec airflow-scheduler \
  python -m crypto_ingest.cli backfill --days 7 -d klines_1m,agg_trades
```

Kiểm tra dung lượng định kỳ:

```bash
sudo docker compose exec airflow-scheduler python -m crypto_ingest.cli status
```

## 8. Backup

Dữ liệu lake nhỏ và bất biến, backup bằng rsync/restic sang ổ khác hoặc S3:

```bash
# ví dụ chạy trên VPS
restic -r s3:... backup ~/bigdata-project/data
```

Tối thiểu nên backup `data/lake` + `data/_state`; Airflow metadata nằm trong volume
`bigdataproject_postgres-db-volume` (có thể backup bằng `pg_dump` nếu cần giữ lịch
sử chạy).
