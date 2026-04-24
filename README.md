# 🤖 Telegram Shop Bot - Bán hàng tự động

Bot Telegram bán hàng tự động với thanh toán QR Code VietQR, Admin Panel quản lý, hỗ trợ auto-check thanh toán qua SePay/Casso.

---

## ✨ Tính năng

### Bot Telegram
- 🛒 Mua hàng nhanh chóng (1 click vào sản phẩm)
- 💳 Thanh toán QR Code VietQR (tất cả ngân hàng VN)
- ⚡ Giao hàng tự động khi nhận tiền
- 📦 Hỗ trợ mua số lượng lớn (gửi file .txt)
- 📣 Broadcast thông báo đến tất cả user
- 📋 Lịch sử đơn hàng

### Admin Panel
- 🔐 Đăng nhập bảo mật
- 📊 Dashboard thống kê
- 📁 Quản lý Categories, Products, Stock
- 📋 Quản lý đơn hàng
- 🏦 Cấu hình ngân hàng
- 🔄 Auto Check thanh toán (SePay/Casso)
- 💾 Backup/Restore database
- 👥 Quản lý Users
- 📣 Gửi Broadcast

---

## 📁 Cấu trúc Source

```
shopbot/
├── app.py                 # Code chính
├── requirements.txt       # Thư viện cần cài
├── templates/
│   ├── admin.html        # Giao diện Admin Panel
│   └── login.html        # Trang đăng nhập
├── Procfile              # Config cho Railway/Heroku
├── railway.toml          # Config Railway
├── .env.example          # Mẫu file cấu hình
├── .gitignore            # File ignore
└── README.md             # Hướng dẫn này
```

---

## ⚙️ Biến môi trường

### Bắt buộc

| Biến | Mô tả | Ví dụ |
|------|-------|-------|
| `BOT_TOKEN` | Token từ @BotFather | `123456:ABC-DEF...` |
| `ADMIN_ID` | Telegram ID của admin | `123456789` |

### Admin Panel

| Biến | Mô tả | Mặc định |
|------|-------|----------|
| `ADMIN_USERNAME` | Tên đăng nhập | `admin` |
| `ADMIN_PASSWORD` | Mật khẩu | `admin123` |

### Thanh toán tự động

| Biến | Mô tả | Ghi chú |
|------|-------|---------|
| `SEPAY_API_KEY` | API Key SePay | Lấy từ my.sepay.vn |
| `CASSO_API_KEY` | API Key Casso | Lấy từ casso.vn |

### Liên hệ hỗ trợ

| Biến | Mô tả | Ví dụ |
|------|-------|-------|
| `ZALO_LINK` | Link Zalo | `https://zalo.me/0123456789` |
| `TELEGRAM_CHANNEL` | Link Telegram | `https://t.me/your_channel` |

---

## 🚀 Hướng dẫn Cài đặt

### Bước 1: Tạo Bot Telegram

1. Mở Telegram, tìm **@BotFather**
2. Gửi `/newbot`
3. Đặt tên bot và username
4. **Lưu lại Bot Token**

### Bước 2: Lấy Telegram ID

1. Mở Telegram, tìm **@userinfobot**
2. Gửi `/start`
3. **Lưu lại ID**

---

## 🌐 Deploy lên Railway

### Bước 1: Tạo tài khoản
Vào [railway.app](https://railway.app) → Đăng ký bằng GitHub

### Bước 2: Tạo Project
1. Click **New Project** → **Empty Project**
2. Click **New** → **GitHub Repo** hoặc dùng Railway CLI

### Bước 3: Cấu hình biến môi trường

Vào service → **Variables** → Thêm:

```
BOT_TOKEN=your_bot_token
ADMIN_ID=your_telegram_id
ADMIN_USERNAME=admin
ADMIN_PASSWORD=your_password
SEPAY_API_KEY=your_sepay_key
ZALO_LINK=https://zalo.me/your_phone
TELEGRAM_CHANNEL=https://t.me/your_channel
```

### Bước 4: Truy cập

- **Admin Panel**: `https://your-domain.railway.app/admin`
- **Webhook SePay**: `https://your-domain.railway.app/webhook/sepay`

---

## 🖥️ Deploy lên VPS

### Bước 1: Cài môi trường

```bash
sudo apt update && sudo apt upgrade -y
sudo apt install python3 python3-pip python3-venv -y
```

### Bước 2: Upload source

```bash
mkdir -p /home/shopbot
cd /home/shopbot
# Upload source code vào đây
```

### Bước 3: Cài đặt

```bash
cd /home/shopbot
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### Bước 4: Tạo file .env

```bash
cp .env.example .env
nano .env
```

Điền đầy đủ thông tin:

```env
BOT_TOKEN=your_bot_token
ADMIN_ID=your_telegram_id
ADMIN_USERNAME=admin
ADMIN_PASSWORD=your_password
SEPAY_API_KEY=your_sepay_key
ZALO_LINK=https://zalo.me/your_phone
TELEGRAM_CHANNEL=https://t.me/your_channel
```

### Bước 5: Chạy thử

```bash
python3 app.py
```

### Bước 6: Chạy nền với Systemd

```bash
sudo nano /etc/systemd/system/shopbot.service
```

Nội dung:

```ini
[Unit]
Description=Telegram Shop Bot
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/home/shopbot
EnvironmentFile=/home/shopbot/.env
ExecStart=/home/shopbot/venv/bin/python app.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Kích hoạt:

```bash
sudo systemctl daemon-reload
sudo systemctl enable shopbot
sudo systemctl start shopbot
```

### Bước 7: (Tùy chọn) Nginx + SSL

```bash
sudo apt install nginx certbot python3-certbot-nginx -y
sudo nano /etc/nginx/sites-available/shopbot
```

```nginx
server {
    listen 80;
    server_name yourdomain.com;

    location / {
        proxy_pass http://127.0.0.1:8080;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
    }
}
```

```bash
sudo ln -s /etc/nginx/sites-available/shopbot /etc/nginx/sites-enabled/
sudo nginx -t && sudo systemctl restart nginx
sudo certbot --nginx -d yourdomain.com
```

---

## ⚙️ Cấu hình sau Deploy

### 1. Đăng nhập Admin Panel

- URL: `https://your-domain/admin`
- Đăng nhập với tài khoản đã cấu hình

### 2. Cấu hình Bank

Vào **Cấu hình Bank** → Điền:
- Mã ngân hàng (xem bảng bên dưới)
- Số tài khoản
- Tên chủ TK (KHÔNG DẤU, IN HOA)

### 3. Tạo sản phẩm

1. **Categories** → Thêm danh mục
2. **Sản phẩm** → Thêm sản phẩm
3. **Kho hàng** → Thêm stock (mỗi dòng 1 tài khoản)

### 4. Cấu hình Auto Check

#### SePay (Khuyên dùng):
1. Đăng ký [my.sepay.vn](https://my.sepay.vn)
2. Liên kết ngân hàng
3. **Cài đặt** → **Webhook**:
   - URL: `https://your-domain/webhook/sepay`
   - Kiểu chứng thực: **Không chứng thực**
   - Bỏ qua nếu không có code: **Không**

#### Casso:
1. Đăng ký [casso.vn](https://casso.vn)
2. Thêm webhook: `https://your-domain/webhook/casso`

---

## 🏦 Mã ngân hàng VietQR

| Ngân hàng | Mã |
|-----------|-----|
| BIDV | 970418 |
| Vietcombank | 970436 |
| Techcombank | 970407 |
| MB Bank | 970422 |
| ACB | 970416 |
| VPBank | 970432 |
| TPBank | 970423 |
| Sacombank | 970403 |
| Agribank | 970405 |
| VietinBank | 970415 |

Xem đầy đủ: [api.vietqr.io/v2/banks](https://api.vietqr.io/v2/banks)

---

## 💾 Backup & Restore

### Railway (QUAN TRỌNG!)

⚠️ Railway xóa data khi redeploy!

**Trước redeploy:**
1. Admin Panel → **Cài đặt**
2. Click **Tải Database**

**Sau redeploy:**
1. Admin Panel → **Cài đặt**
2. Click **Khôi phục** → Chọn file .db

### VPS

Data tự động lưu, không cần backup thủ công.

---

## 🔧 Lệnh quản lý (VPS)

| Lệnh | Mô tả |
|------|-------|
| `sudo systemctl start shopbot` | Khởi động |
| `sudo systemctl stop shopbot` | Dừng |
| `sudo systemctl restart shopbot` | Khởi động lại |
| `sudo systemctl status shopbot` | Xem trạng thái |
| `sudo journalctl -u shopbot -f` | Xem logs |

---

## ❓ Xử lý lỗi

### Bot không phản hồi
- Kiểm tra `BOT_TOKEN`
- Xem logs

### "Conflict: terminated by other getUpdates"
- Bot đang chạy nhiều nơi
- Chỉ chạy 1 instance

### Không nhận thanh toán tự động
- Kiểm tra webhook URL
- SePay chọn "Không chứng thực"
- Thử Manual Check

### Liên hệ Admin không hiện link
- Kiểm tra đã điền `ZALO_LINK` và `TELEGRAM_CHANNEL` chưa

---

## 📞 Hỗ trợ

Liên hệ người bán để được hỗ trợ.

---

**Chúc bạn kinh doanh thành công! 🎉**
