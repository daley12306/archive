DREMIO_HOST     = "localhost"       # nếu chạy script từ máy host
# DREMIO_HOST   = "dremio"         # nếu chạy từ trong container khác
DREMIO_PORT     = 9047
DREMIO_USER     = "admin"
DREMIO_PASSWORD = "$tr0ngPa$$Word" 

GOLD_SPACE      = "Nessie.gold"

QDRANT_HOST       = "localhost"
QDRANT_PORT       = 6333
COLLECTION_NAME   = "job_recommend"

# e5-small  (~500MB)  — dùng khi test, nhanh hơn
# e5-large  (~2.2GB)  — dùng khi production, chất lượng tốt hơn
EMBED_MODEL       = "intfloat/multilingual-e5-small"
VECTOR_SIZE       = 384    # e5-small=384, e5-large=1024

# Prefix bắt buộc của multilingual-e5
PASSAGE_PREFIX    = "passage: "   # dùng khi embed job
QUERY_PREFIX      = "query: "     # dùng khi embed CV

BATCH_SIZE        = 64    # số job embed mỗi lần, tăng nếu RAM đủ
