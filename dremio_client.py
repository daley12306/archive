"""
dremio_client.py — Wrapper cho Dremio REST API v3

Dremio v3 SQL là async:
  POST /api/v3/sql       → job_id
  GET  /api/v3/job/{id}  → poll state
  GET  /api/v3/job/{id}/results → rows
"""
import time
import requests
from config import DREMIO_HOST, DREMIO_PORT, DREMIO_USER, DREMIO_PASSWORD


class DremioClient:
    def __init__(self):
        self.base  = f"http://{DREMIO_HOST}:{DREMIO_PORT}"
        self.token = self._login()

    def _login(self) -> str:
        resp = requests.post(
            f"{self.base}/apiv2/login",
            json={"userName": DREMIO_USER, "password": DREMIO_PASSWORD},
            timeout=10,
        )
        if resp.status_code != 200:
            raise ConnectionError(
                f"Dremio login thất bại ({resp.status_code}): {resp.text}\n"
                f"Kiểm tra DREMIO_USER và DREMIO_PASSWORD trong config.py"
            )
        print(f"  Dremio login OK ({DREMIO_HOST}:{DREMIO_PORT})")
        return resp.json()["token"]

    def query(self, sql: str) -> list[dict]:
        """
        Dremio REST API v3 là async — 3 bước:
          1. POST /api/v3/sql        → nhận job_id
          2. Poll GET /api/v3/job/{id} đến state = COMPLETED
          3. GET /api/v3/job/{id}/results?offset=N&limit=500
        """
        headers = {"Authorization": f"_dremio{self.token}"}

        # Bước 1: Submit query
        resp = requests.post(
            f"{self.base}/api/v3/sql",
            headers=headers,
            json={"sql": sql},
            timeout=30,
        )
        if resp.status_code != 200:
            raise RuntimeError(
                f"Submit query thất bại ({resp.status_code}): {resp.text}"
            )

        job_id = resp.json().get("id")
        if not job_id:
            raise RuntimeError(f"Không nhận được job_id từ Dremio: {resp.text}")

        # Bước 2: Poll đến khi hoàn thành (timeout 60s)
        for _ in range(60):
            status = requests.get(
                f"{self.base}/api/v3/job/{job_id}",
                headers=headers,
                timeout=10,
            ).json()
            state = status.get("jobState", "")

            if state == "COMPLETED":
                break
            elif state in ("FAILED", "CANCELED"):
                error = status.get("errorMessage", "unknown error")
                raise RuntimeError(f"Dremio job {state}: {error}")
            time.sleep(1)
        else:
            raise RuntimeError(f"Dremio query timeout sau 60s (job_id={job_id})")

        # Bước 3: Lấy kết quả có phân trang
        rows, offset, page_size = [], 0, 500
        while True:
            result = requests.get(
                f"{self.base}/api/v3/job/{job_id}/results",
                headers=headers,
                params={"offset": offset, "limit": page_size},
                timeout=30,
            )
            if result.status_code != 200:
                raise RuntimeError(f"Lấy results thất bại: {result.text}")

            data      = result.json()
            batch     = data.get("rows", [])
            row_count = data.get("rowCount", 0)
            rows     += batch

            if not batch or offset + page_size >= row_count:
                break
            offset += page_size

        return rows
