# Spark Bronze → Silver → Gold

Pipeline transform hiện chạy **PySpark 4.0.2**, ghi bảng **Parquet/Snappy** trên MinIO.
Mô hình vẫn có 4 bảng Silver, 5 dimension, 5 fact và 2 bảng tổng hợp ngày.
Bronze consumer, API, Kafka producer, main.py và main2.py không thay đổi.

## Cấu trúc

```text
transforms/
├── pipeline.py          # Entry point CLI và hàm gọi từ Airflow
├── storage.py           # MinIO: kiểm tra bucket, danh sách object, manifest JSON
├── silver.py            # Quy tắc chuẩn hóa từng record (chạy trên executor)
├── model.py             # Định danh khóa; model Python tham chiếu cho kiểm thử
├── gold.py              # Tổng hợp Python tham chiếu cho kiểm thử
├── spark/
│   ├── session.py       # SparkSession, S3A, phân phối Python modules
│   ├── schemas.py       # Schema tường minh
│   ├── silver.py        # Parse, quarantine, window dedup, kiểu timestamp/date
│   ├── gold.py          # DataFrame fact/dimension, groupBy, kiểm tra PK/FK
│   └── jobs.py          # Đọc Bronze, ghi Parquet, công bố snapshot
└── requirements.txt
```

Không gọi build_silver(list), build_star_schema(list) hay build_gold(list) trong job
production. Chúng chỉ còn làm tham chiếu cho tests. Scalar UDF chuẩn hóa/key chạy
trên executor, mỗi lần xử lý một record; window/join/groupBy chạy bằng Spark SQL.
Không collect dataset/toPandas về driver. Driver chỉ giữ danh sách object và manifest.

## Cài đặt

Cần Java 17 hoặc 21 và Python 3.9 trở lên; driver/executor phải cùng Python và PySpark.
Trong môi trường chạy transform:

```bash
pip install -r transforms/requirements.txt
```

Nếu dùng Airflow, cài requirements này trong môi trường thực thi task sau khi thiết
lập Airflow, giữ phiên bản apache-airflow đã cài khi resolver xử lý dependencies.
Không cần Spark provider: task hiện tạo SparkSession và chạy ở client mode.

Chuẩn bị sẵn bucket `bronze`, `silver`, `gold`; quyền list/get Bronze và get/put các
bucket output. Bronze cần có object `.jsonl.gz` do kafka.bronze_consumer ghi.
Dùng `.env` hiện có, thêm cấu hình Spark trong `.env.example`:

```text
SPARK_MASTER=local[2]
SPARK_SHUFFLE_PARTITIONS=8
SPARK_S3_PACKAGES=org.apache.hadoop:hadoop-aws:3.4.1
MINIO_ENDPOINT=localhost:9000
MINIO_SECURE=false
MINIO_REGION=us-east-1
MINIO_SILVER_BUCKET=silver
MINIO_GOLD_BUCKET=gold
MINIO_TRANSFORM_PREFIX=environment-v1
```

Điền MINIO_ACCESS_KEY/MINIO_SECRET_KEY qua secret/environment; không commit khóa.
S3A tải hadoop-aws và dependency từ Maven khi khởi chạy. Phiên bản hadoop-aws phải
khớp Hadoop của Spark distribution. Mặc định được chọn cho bản PySpark ghim ở trên;
đổi distribution thì kiểm tra và đặt lại SPARK_S3_PACKAGES. Môi trường offline cần
cài sẵn jars và đặt biến đó thành chuỗi rỗng để không resolve Maven.

## Chạy

```bash
python -m transforms.pipeline
# Hoặc qua Airflow đã chạy và đã nhận DAG:
python main2.py run --sources transform
python main2.py status --sources transform
```

Airflow giữ dependency `transform_silver → transform_gold`; mỗi task tạo và đóng
SparkSession riêng. XCom chỉ mang đường dẫn manifest Silver. DAG max_active_runs=1;
không chạy CLI trực tiếp đồng thời trên cùng prefix. Consumer Kafka → MinIO tiếp tục
chạy độc lập; object đến sau danh sách đầu vào được xử lý ở lượt sau.

Local[2] là Spark thật trên một máy, không phải cụm phân tán. Để chạy cụm, đặt
SPARK_MASTER (ví dụ spark://spark-master:7077), cấu hình mạng/driver resources trong
spark-defaults.conf hoặc môi trường triển khai. Driver và mọi executor phải truy cập
được MinIO; localhost của mỗi container không phải cùng một máy. session.py đóng gói
transforms/common thành zip gửi tới executors, không gửi .env. Cần môi trường executor
cùng Python, JDK và các S3A jars phù hợp. Chưa cung cấp deployment cụm Spark/Kubernetes.

## Dữ liệu và mô hình

Xem [DATA_MODEL.md](DATA_MODEL.md) về grain/PK/FK và quy tắc cập nhật.

Silver normalize thời gian UTC, tọa độ, đổi km/h → m/s và °F → °C. Số đo khác giữ đơn
vị gốc, không tự suy ra AQI. Window loại trùng transport và chọn phiên bản hợp lệ mới
nhất theo source/event_id. Timestamp/date lưu thành kiểu Parquet tương ứng.

Bản ghi lỗi được lưu trong bảng rejects với raw_line, bronze_object và reason.
File gzip hỏng hoặc lỗi đọc storage làm job thất bại, không âm thầm bỏ qua.
Không có Bronze hoặc không có record hợp lệ thì không công bố snapshot mới.

Gold có 10 bảng fact/dimension và daily_events/daily_metrics. Tên và khóa giữ theo mô
hình v2; sample_mean không áp dụng góc. Count điểm nóng không phải count vụ cháy;
trung bình mẫu không phải trung bình theo thời gian hoặc báo cáo chất lượng môi trường.

## Snapshot v3 và phục hồi

```text
silver/<prefix>/snapshots/<id>/silver_<entity>/part-*.snappy.parquet
silver/<prefix>/snapshots/<id>/rejects/part-*.snappy.parquet
silver/<prefix>/snapshots/<id>/manifest.json
gold/<prefix>/snapshots/<id>/gold-attempts/<attempt>/dim_*/part-*.snappy.parquet
gold/<prefix>/snapshots/<id>/gold-attempts/<attempt>/fact_*/part-*.snappy.parquet
gold/<prefix>/snapshots/<id>/gold-attempts/<attempt>/daily_*/part-*.snappy.parquet
gold/<prefix>/snapshots/<id>/gold-attempts/<attempt>/manifest.json
gold/<prefix>/latest.json
```

Reader mở latest.json trước rồi đọc từng directory Parquet trong gold_tables hoặc
silver_tables; không quét tất cả snapshots. Manifest chứa format=parquet, engine=pyspark,
schema_version=3.0. Entries của bảng chứa key directory và format; không còn row count
riêng từng bảng. Tổng silver_count/reject_count vẫn có trong manifest.

Chỉ publish pointer sau khi ghi tất cả bảng Gold và kiểm tra PK/FK. Gold retry tạo
attempt directory riêng để không ghi đè bảng đã công bố. Lỗi trước publish giữ nguyên
pointer trước; snapshot/attempt dở dang có thể cần dọn bằng tác vụ riêng.
Migration: chạy lại từ Bronze để tạo v3; không đọc manifest Silver v1/v2 bằng job Gold
mới. Snapshot cũ giữ nguyên, consumer báo cáo cần chuyển từ JSONL sang Parquet.

## Giới hạn còn lại

Đã chuyển tính toán và dữ liệu sang Spark, nhưng vẫn **full rebuild** từ Bronze mỗi
lượt; chưa incremental MERGE, Delta/Iceberg hoặc SCD2 đầy đủ. Không xóa lịch sử Bronze
trước khi có cơ chế merge. Input object cần bất biến trong lượt đọc. Danh sách tên
object vẫn ở driver; hàng triệu object cần thêm partition/catalog để tránh listing lớn.

Gzip Bronze không split được bên trong một file; nhiều file batch tạo đơn vị đọc song
song. Không dùng coalesce(1); số part output do Spark quyết định. Cần theo dõi small
files và thiết lập compaction/partition theo kích thước thực tế khi vận hành.

## Kiểm thử

```bash
python -B -m unittest discover -s tests -p 'test_*.py' -v
python -B -m unittest discover -s airflow/tests -p 'test_*.py' -v
```

test_spark_pipeline chạy Spark thật local, đọc gzip, ghi/đọc Parquet, so khóa mô hình
với bản Python tham chiếu và kiểm tra lỗi Gold không thay pointer. Dùng filesystem
local nên chưa thay thế kiểm thử S3A với MinIO thật. Nếu chưa cài PySpark, nhóm Spark
được đánh dấu skip; các unit tests khác vẫn chạy.
