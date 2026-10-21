# Chia sẻ Airflow UI cho người khác

UI đang chạy tại `http://localhost:8080` trên máy này (container
`bigdataproject-airflow-webserver-1`, port `0.0.0.0:8080`).

## 1. Trong cùng mạng LAN

Máy này: `192.168.1.17`. Người cùng wifi/office mở:

```
http://192.168.1.17:8080
```

Kiểm tra firewall: `sudo ufw status`. Nếu UFW đang bật:

```bash
sudo ufw allow 8080/tcp
```

Đây là cách nhanh nhất, không lộ ra internet. Chỉ dùng khi tin tưởng mạng LAN.

## 2. Internet — Cloudflare Quick Tunnel (đang dùng)

Không cần mở port/router, không cần domain:

```bash
cd "/home/hai-huynh/bigdata project"
make share        # in ra URL https://<random>.trycloudflare.com
make unshare      # dừng tunnel
```

Script dùng `cloudflared tunnel --url http://localhost:8080`, chạy detached
(`logs/cloudflared.out`, `logs/cloudflared.log`).

Lưu ý:

- URL **đổi mỗi lần restart** tunnel và chỉ sống khi process còn chạy.
- Bất kỳ ai có URL đều thấy trang login. Vì vậy phải dùng mật khẩu mạnh và
  không share tài khoản admin.
- Quick tunnel phù hợp demo, không phù hợp production lâu dài.
- Nếu QUIC bị chặn: thêm `--protocol http2` vào lệnh cloudflared.

## 3. Internet — Named Tunnel + domain (ổn định, nên dùng lâu dài)

Cần tài khoản Cloudflare và một domain (có thể mua rẻ hoặc dùng subdomain).

```bash
cloudflared tunnel login
cloudflared tunnel create airflow
cloudflared tunnel route dns airflow airflow.example.com
```

`~/.cloudflared/config.yml`:

```yaml
tunnel: airflow
credentials-file: /home/hai-huynh/.cloudflared/<TUNNEL_ID>.json
ingress:
  - hostname: airflow.example.com
    service: http://localhost:8080
  - service: http_status:404
```

Chạy nền bằng systemd:

```bash
sudo cloudflared service install
sudo systemctl enable --now cloudflared
```

Sau đó thêm **Cloudflare Access** (Zero Trust → Access → Application) để chỉ
email được phép mới vào được, kể cả trước trang login của Airflow.

Khi dùng domain cố định, nên set thêm:

```env
AIRFLOW__WEBSERVER__BASE_URL=https://airflow.example.com
```

rồi `docker compose up -d`.

## 4. Internet — Tailscale (riêng tư, an toàn nhất)

```bash
curl -fsSL https://tailscale.com/install.sh | sh
sudo tailscale up
```

Cài Tailscale trên máy người nhận, mời vào tailnet, rồi họ mở:

```
http://<tailscale-ip-cua-may-nay>:8080
```

Không có URL công khai, không cần mở port. Phù hợp nhóm nhỏ dùng lâu dài.

## 5. Tài khoản và phân quyền

Đã tạo:

| User | Role | Quyền |
|---|---|---|
| `admin` | Admin | toàn quyền, **không share** |
| `viewer` | Viewer | chỉ xem DAG, grid, log; không sửa/trigger |

Tạo thêm tài khoản viewer cho từng người:

```bash
cd "/home/hai-huynh/bigdata project"
docker compose exec airflow-scheduler airflow users create \
  --username alice --firstname Alice --lastname Nguyen \
  --role Viewer --email alice@example.com --password 'MatKhauManh'
```

Role khác: `User` (xem + tự sửa profile), `Op` (được trigger/clear task),
`Admin` (toàn quyền). Chỉ cấp `Op`/`Admin` cho người thực sự cần.

Đổi mật khẩu:

```bash
docker compose exec airflow-scheduler airflow users reset-password -u viewer
```

## 6. Checklist trước khi share

- [ ] Đã đổi mật khẩu `admin` (không dùng `admin/admin`).
- [ ] Người nhận dùng tài khoản `Viewer` riêng, không dùng chung admin.
- [ ] `AIRFLOW__WEBSERVER__EXPOSE_CONFIG=false` (đã set trong compose).
- [ ] Nếu share internet: ưu tiên Cloudflare Access hoặc Tailscale.
- [ ] Không đưa URL/credentials vào git, chat công khai, screenshot.
- [ ] Dừng tunnel khi không cần: `make unshare`.
