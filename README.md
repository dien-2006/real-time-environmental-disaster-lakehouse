# Environment API → Kafka

Poll API nguồn, bọc metadata chung và giữ bản ghi nguồn trong `raw` để lưu Bronze.
Chưa triển khai Bronze sink/Silver. Đây là polling HTTP, không phải WebSocket/SSE.

## Cài đặt và chạy

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
# Nếu chưa có .env: sao chép .env.example thành .env rồi điền khóa.
docker compose up -d
python main.py --sources usgs --once
# Terminal khác để đọc sự kiện:
python -m kafka.consumer --topic earthquake
python main.py --sources usgs open_meteo nasa_firms openaq
```

Mặc định `python main.py` chạy USGS, không cần API key. NASA và OpenAQ yêu cầu khóa
trong `.env`. Các nguồn được chọn phải đủ cấu hình; lỗi startup trả exit code 1.
Runner tạo topic trước khi lấy dữ liệu (3 partition, replication factor 3).
Với broker đơn, dùng `--replication-factor 1` cho topic mới.

| Nguồn | Topic | Chu kỳ mặc định |
|---|---|---|
| usgs | earthquake | 60 giây |
| open_meteo | weather | 900 giây |
| nasa_firms | nasa_firms | 1800 giây |
| openaq | air_quality | 900 giây |

`--interval 120` thay chu kỳ cho các nguồn đã chọn. Chu kỳ là thời gian chờ sau mỗi
lượt thu thập; cần cân đối quota API. Ctrl+C dừng sau lượt đang xử lý.
Mỗi nguồn có một thread, HTTP session và producer riêng.

Host: `KAFKA_BOOTSTRAP_SERVERS=localhost:9092,localhost:9093,localhost:9094`.
Trong cùng mạng Docker: `kafka1:29092,kafka2:29092,kafka3:29092`.
Kafka UI: http://localhost:8080 — chọn topic để kiểm tra message.

## Cấu trúc message

- `schema_version`: phiên bản envelope (1.0).
- `source`, `event_type`: nguồn và loại dữ liệu.
- `event_id`: ID ổn định từ danh tính bản ghi nguồn.
- `ingested_at`: thời gian thu thập UTC.
- `payload_hash`: nhận biết nội dung mới hoặc sửa đổi.
- `context`: thông tin trạm/sensor hoặc tên địa điểm.
- `raw`: dữ liệu nguồn chưa chuẩn hóa nghiệp vụ; NASA là dictionary của một dòng CSV,
  USGS là GeoJSON feature, OpenAQ là measurement, Open-Meteo là response theo địa điểm.

Thời gian quan sát, kiểu dữ liệu, đơn vị được chuẩn hóa khi Bronze → Silver.
Open-Meteo hash phần `current` để thời gian xử lý HTTP trong response không tạo bản
cập nhật giả. Thay đổi metadata ngoài `current` không tự phát một event mới.

## Giao nhận và phạm vi dữ liệu

SQLite `.state/<source>.sqlite3` ghi phiên bản mới nhất sau khi Kafka xác nhận từng
event. Nhận response → lấy một event → gửi Kafka → chờ xác nhận → lấy event tiếp theo.
Gửi lỗi không cập nhật trạng thái event. Chạy lại cùng dữ
liệu sẽ bỏ qua; thay đổi nội dung cùng ID sẽ được gửi lại.
Crash giữa Kafka ACK và SQLite commit vẫn có thể gây trùng: consumer cần chống
trùng theo `event_id` + `payload_hash`. Chỉ chạy một runner cho mỗi nguồn/state.
State hiện chưa có chính sách dọn dữ liệu; cần bổ sung theo retention khi vận hành lâu dài.

USGS dùng feed một ngày; NASA lấy một ngày, mặc định vùng Việt Nam (`NASA_FIRMS_AREA=world`
nếu cần toàn cầu). OpenAQ phân trang quốc gia/trạm/measurements và đọc lại 24 giờ
để thu nhận dữ liệu đến muộn. Đây chưa phải cơ chế backfill đầy đủ: gián đoạn quá cửa
sổ hoặc dữ liệu đến muộn hơn cửa sổ cần tác vụ lấy bù riêng.

`producers/` và DAG Airflow cũ không được runner này sử dụng.
Không log URL lỗi HTTP vì URL NASA chứa API key.

## Kiểm thử độc lập, không cần Kafka hoặc API key

```bash
python3 -m unittest discover -s tests -p 'test_*.py' -v
```
