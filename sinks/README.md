# Kafka → MinIO Bronze

Producer/main/main2 và consumer xem message được giữ nguyên. Worker mới:

```bash
pip install -r requirements.txt
python -m kafka.bronze_consumer --topics earthquake
# Hoặc cả bốn topic (tạo các topic trước):
python -m kafka.bronze_consumer
```

Cần MinIO đang chạy và bucket `bronze` đã được tạo (qua console hoặc công cụ quản trị).
Worker không tự tạo bucket hay tài khoản. Điền cấu hình `.env` theo `.env.example`:
`MINIO_ENDPOINT` (không có scheme), `MINIO_ACCESS_KEY`, `MINIO_SECRET_KEY`,
`MINIO_SECURE`, `MINIO_BRONZE_BUCKET`, `MINIO_BRONZE_PREFIX`.
Account cần quyền kiểm tra bucket và ghi object. Worker này chưa cung cấp MinIO server.

## Batching và ghi dữ liệu

Mỗi topic/partition có batch riêng: mặc định tối đa 500 bản ghi, khoảng 5 MB JSONL
chưa nén hoặc tuổi batch 10 giây. Có thể đổi bằng `--batch-size`, `--batch-bytes`,
`--batch-seconds`. Thời gian là ngưỡng flush được kiểm tra trong vòng poll, không
phải cam kết latency khi storage chậm. Một record lớn hơn giới hạn được ghi riêng;
bộ nhớ ứng dụng phụ thuộc số partition được gán và kích thước message lớn nhất.

Object ví dụ trong bucket `bronze`:

```text
environment-v1/topic=earthquake/partition=0/offsets=1200-1699.jsonl.gz
```

Dùng topic/partition/offset, không dùng ngày giờ hiện tại trong key, để khi retry
cùng batch có cùng đường dẫn. Mỗi dòng gồm `event` (metadata + raw nguyên cấu trúc),
`kafka` (topic, partition, offset, timestamp, key/headers base64) và `value_base64`
để giữ bytes gốc. JSON lỗi vẫn được lưu kèm `decode_error`; tombstone giữ value null.
Chưa chuẩn hóa nghiệp vụ hoặc đơn vị. Không dùng source do message cung cấp để tạo
đường dẫn object; topic xác định nguồn pipeline theo cấu hình hiện có.

## Giao nhận và phục hồi

Tắt auto commit và auto offset store. Upload thành công rồi commit đồng bộ offset
cuối + 1 cho đúng partition. Ghi lỗi hoặc commit lỗi: dừng với exit code 1, không
commit vượt qua batch lỗi. SDK HTTP thử lại có giới hạn; dùng process supervisor
để khởi động lại worker khi vận hành. Ctrl+C/SIGTERM flush phần còn lại rồi đóng.
Khi revoke/lost partition, bỏ buffer chưa commit để chủ sở hữu mới đọc lại.

Đây là at-least-once, KHÔNG exactly-once: upload thành công rồi crash trước commit
có thể tạo dữ liệu trùng. Tên object ổn định chỉ giúp khi khoảng offset giống nhau;
sau restart, ranh giới batch có thể đổi và tạo object chồng lấn. Silver chống trùng
bằng `(topic, partition, offset)`; trùng nghiệp vụ dùng thêm event_id/payload_hash.
Đổi prefix khi chuyển cluster hoặc xóa/tạo lại topic để tránh offset tái sử dụng.
Không chạy nhiều consumer group ghi cùng prefix. Cùng group thì Kafka chia partition.

Group mặc định `minio-bronze-v1` khác consumer chỉ in message để không kế thừa offset
đã commit mà chưa lưu Bronze. `earliest` chỉ áp dụng khi group chưa có offset hợp lệ;
Kafka retention vẫn giới hạn dữ liệu có thể đọc lại.

Batch upload đồng bộ tạo backpressure: storage chậm thì tốc độ đọc giảm. Nếu chậm
quá `max.poll.interval.ms` (15 phút), partition có thể bị thu hồi; worker sẽ dừng khi
commit thất bại và replay khi chạy lại. Theo dõi lag, tốc độ upload và dung lượng.

## Kiểm thử

```bash
python3 -B -m unittest discover -s tests -p 'test_bronze_batch.py' -v
```

Kiểm thử mock xác nhận thứ tự upload/commit, lỗi, rebalance, các ngưỡng batch và
payload. Chưa thay thế kiểm thử tích hợp với Kafka/MinIO thật.
