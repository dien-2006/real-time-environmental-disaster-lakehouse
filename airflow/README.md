# Khởi chạy pipeline qua main2.py

`main.py` giữ nguyên cho worker Python. `main2.py` ở thư mục gốc là entry point
quản lý bản Airflow; mọi logic triển khai nằm trong `airflow/`.

## Chạy thử trên một máy

Ví dụ cài bằng Python 3.12, trong môi trường riêng (không thay môi trường main.py):

```bash
python3.12 -m venv .venv-airflow
source .venv-airflow/bin/activate
pip install -r airflow/requirements.txt --constraint https://raw.githubusercontent.com/apache/airflow/constraints-3.1.8/constraints-3.12.txt
python main2.py start
```

Lệnh `start` chạy Airflow standalone ở foreground, dành cho phát triển/so sánh.
UI mặc định http://localhost:8081, tránh cổng Kafka UI 8080. Thông tin đăng nhập
xem ở đầu ra khởi động Airflow. Ctrl+C dừng standalone.
Kafka phải được khởi động riêng theo Docker Compose hiện có.

Terminal khác, kích hoạt cùng môi trường rồi chạy:

```bash
python main2.py check
python main2.py run --sources usgs
python main2.py status --sources usgs
python main2.py pause --sources usgs
```

- Đợi scheduler nhận diện DAG trước khi `run`.
- `run` bật lịch định kỳ và gửi thêm một lượt chạy, không chờ task hoàn tất.
- `status` hiển thị các lượt chạy; xem log task trong Airflow UI.
- `pause` dừng lập lịch tiếp theo, không hủy task đang chạy và không dừng Kafka.
- `check` in lỗi import DAG; cần đọc kết quả, không chỉ dựa vào exit code.
- Thêm `--dry-run` để xem lệnh mà không cần cài hoặc khởi chạy Airflow.
- Có thể chọn nhiều nguồn: `--sources usgs open_meteo nasa_firms openaq`.
- API key được client đọc từ môi trường hoặc `.env` hiện có; không in key ra CLI.

## Các lớp trách nhiệm

```text
main2.py
  └─ environment_ingestion/cli.py        Lệnh quản lý và gọi Airflow CLI
       └─ runtime.py                    Môi trường, đường dẫn, giới hạn song song

Airflow scheduler
  └─ api_kafka_ingestion.py              Đăng ký DAG
       └─ dag_factory.py                Chính sách lịch/retry/task
            └─ runner.py                Một lượt lấy API và gửi Kafka
                 └─ common.ingestion    Giao nhận và checkpoint hiện có
```

`sources.py` là nơi duy nhất khai báo nguồn cho cả CLI và DAG.
`requirements.txt` ghim Airflow 3.1.8; cài với constraints tương ứng Python.

## Khi triển khai vận hành

Không dùng standalone làm cấu hình production. Chạy API server, scheduler và
DAG processor dưới process/container supervisor, cấu hình metadata DB PostgreSQL,
authentication và lưu logs bền vững theo môi trường. Các process phải dùng cùng
cấu hình, phiên bản code và DAG. CLI chỉ là công cụ quản lý, không phải supervisor.

`runtime.py` cung cấp mặc định local; biến môi trường đã đặt được ưu tiên. Khi tách
API server sang máy khác, đặt cả `AIRFLOW__API__BASE_URL` và
`AIRFLOW__CORE__EXECUTION_API_SERVER_URL` phù hợp. `AIRFLOW_HOME` mặc định nằm ở
`.state/airflow`, checkpoint ở `.state/airflow-ingestion`, không lẫn worker cũ.
Giới hạn SQLite và phạm vi backfill bên dưới vẫn áp dụng; việc tách file chưa biến
checkpoint SQLite thành cơ chế phân tán. Repo chưa cung cấp deployment production.

---

# Bản điều phối Airflow để so sánh với main.py

Entry point: `dags/api_kafka_ingestion.py`. Không sửa `main.py` hoặc nội dung DAG NASA cũ.

## Cấu trúc

```text
airflow/
├── dags/
│   ├── .airflowignore
│   ├── api_kafka_ingestion.py          # Đăng ký DAG với scheduler
│   ├── environment_ingestion/
│   │   ├── __init__.py
│   │   ├── cli.py                     # Lệnh gọi từ main2.py
│   │   ├── runtime.py                 # Cấu hình môi trường chạy
│   │   ├── sources.py                 # Nguồn, class API, lịch cron
│   │   ├── dag_factory.py             # Tạo DAG/task, retry, concurrency
│   │   └── runner.py                  # Chạy một lượt, quản lý tài nguyên
│   └── nasa_firms_ingestion.py        # Bản cũ, bỏ qua khi discovery
├── tests/
│   └── test_structure.py
└── README.md
```

Luồng gọi: `api_kafka_ingestion.py` → `dag_factory.py` → task chạy `runner.py`
→ `common.ingestion.ingest_once()` → API/Kafka hiện có.

Thêm nguồn: triển khai client theo giao diện hiện có (`TOPIC`, `session`, `fetch()`),
rồi thêm một mục vào `sources.py`. Sửa chính sách retry/concurrency tại `dag_factory.py`.
Các import kết nối API/Kafka và đọc cấu hình runtime chỉ diễn ra lúc task thực thi.
Không tạo package Python tên `airflow` trong repo để tránh trùng thư viện Airflow.

Sử dụng Airflow 3 (`airflow.sdk`), tạo bốn DAG độc lập:

| DAG | Lịch UTC |
|---|---|
| api_kafka_usgs | Mỗi phút |
| api_kafka_open_meteo | Phút 0, 15, 30, 45 |
| api_kafka_openaq | Phút 0, 15, 30, 45 |
| api_kafka_nasa_firms | Phút 0 và 30 |

## So sánh trực tiếp

| main.py | DAG mới |
|---|---|
| SOURCES có số giây chờ | SOURCES có cron |
| worker() chạy vòng while | run_source_once() chạy đúng một lượt |
| stop.wait() sau mỗi lượt | Scheduler lên lịch theo đồng hồ |
| ThreadPoolExecutor | Airflow executor chạy task |
| Thử lại ở vòng tiếp theo | 3 retries, thời gian chờ tăng dần |
| SQLite trong .state | API_KAFKA_STATE_DIR trỏ tới thư mục bền vững |

Cả hai dùng lại `client.fetch()` → `ingest_once()` → Kafka producer.
Không truyền payload qua XCom. Metadata + raw và cơ chế checkpoint được giữ nguyên.
Mỗi DAG chỉ cho một run/task hoạt động cùng lúc; nguồn khác vẫn có thể chạy song song.
Một lượt kéo dài có thể làm lượt tiếp theo chạy muộn. Cron không giống chờ N giây sau
khi lượt trước kết thúc trong main.py.

## Cách đưa vào môi trường Airflow đã có

1. Dùng môi trường Airflow 3 và cài `requirements.txt` của dự án trong môi trường
   thực thi task. Đây không phải cấu hình tự cài hoặc tự khởi chạy Airflow.
2. Đưa mã dự án vào máy/container chạy task, ví dụ `/opt/environment-api`, và thêm
   đường dẫn đó vào `PYTHONPATH` để import được `api`, `common`, `config`, `kafka`.
3. Sao chép toàn bộ nội dung `dags/` (bao gồm `.airflowignore` và package
   `environment_ingestion/`) vào DAG folder của Airflow, đồng bộ lên các thành phần
   thực thi task. Airflow thêm DAG folder vào Python import path.
   Đặt `AIRFLOW__CORE__DAG_IGNORE_FILE_SYNTAX=glob` để dùng `.airflowignore` đi kèm.
   File ignore bỏ qua helper package khi quét DAG và DAG NASA cũ đang import `src.*`;
   helper package vẫn import được bình thường.
4. Cấu hình biến môi trường cho tiến trình chạy task:

   ```text
   PYTHONPATH=/opt/environment-api
   API_KAFKA_STATE_DIR=/opt/environment-api/.state-airflow
   API_KAFKA_REPLICATION_FACTOR=3
   KAFKA_BOOTSTRAP_SERVERS=kafka1:29092,kafka2:29092,kafka3:29092
   NASA_FIRMS_MAP_KEY=<khóa của bạn>
   OPENAQ_API_KEY=<khóa của bạn>
   ```

   Địa chỉ `kafka1:29092` yêu cầu task ở cùng mạng Docker với Kafka.
   Nếu task chạy trên host thì dùng các cổng localhost như main.py.
5. Gắn thư mục state vào đĩa/volume bền vững, có quyền ghi. Bản này phù hợp Airflow
   thực thi task trên một máy (ví dụ LocalExecutor). Không dùng SQLite WAL trên
   network filesystem. Với Celery/Kubernetes nhiều máy, cần thay state bằng DB
   dùng chung trước khi triển khai; file này chưa thực hiện thay đổi đó.
6. Bốn DAG mới mặc định paused. Dừng worker main.py của nguồn tương ứng, rồi bật
   DAG trong Airflow UI. Bắt đầu với `api_kafka_usgs`, không cần API key.

State riêng của bản Airflow có thể gửi lại dữ liệu đã được main.py gửi. Khi so sánh,
consumer cần chấp nhận bản ghi trùng. Không chạy hai bộ điều phối cho cùng nguồn.
`catchup=False` không tự lấy bù dữ liệu API: các client hiện vẫn lấy cửa sổ dữ liệu
hiện tại, không sử dụng data interval của DAG để truy vấn lịch sử.

API tham khảo: https://airflow.apache.org/docs/task-sdk/1.0.6/api.html

## Kiểm thử cấu trúc (không cần cài Airflow)

```bash
python3 -B -m unittest discover -s airflow/tests -p 'test_*.py' -v
```

Kiểm thử dùng mock SDK để kiểm tra đăng ký và ranh giới thực thi; không thay thế
kiểm thử DagBag/scheduler/task trên môi trường Airflow 3 thật.

## DAG biến đổi dữ liệu

`dags/environment_transform.py` định nghĩa pipeline `environment_bronze_silver_gold`:
Silver hoàn thành trước Gold; consumer Kafka → MinIO vẫn chạy độc lập.

```bash
python main2.py run --sources transform
python main2.py status --sources transform
python main2.py pause --sources transform
```

Cần deploy thêm package `transforms/` cùng mã dự án và tạo bucket Silver/Gold.
Xem `transforms/README.md` để hiểu schema, snapshot và giới hạn full rebuild.

## Spark transform

DAG transform hiện dùng PySpark 4.0.2, Parquet và manifest v3. Cài thêm
`transforms/requirements.txt` trong môi trường thực thi task; cần Java 17/21.
Mỗi task mở/đóng SparkSession, dùng `SPARK_MASTER` chọn local hoặc cluster.
Xem `transforms/README.md` cho S3A, triển khai executor và migration snapshot.
