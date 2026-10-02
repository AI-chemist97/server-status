# server-status

Rev. 0 | Created: 2026-10-02 | Updated: 2026-10-02 16:00 KST

## 1. Purpose

서버의 CPU, memory, disk 사용률과 uptime을 주기적으로 `data/server.json`에 기록하고 이 repository에 push한다. 블로그 page가 그 file을 읽어 서버 상태를 보여준다.

## 2. Setup

서버에서 이 repository를 clone한 뒤 아래 순서로 실행한다.

```bash
git clone <REPO_URL> server-status
cd server-status
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
.venv/bin/python collector/collect.py --output-folder . --run serve --interval-minutes 15
```

- Push 권한: 이 repository에 write 권한이 있는 deploy key 또는 token
- Commit 작성자: 서버의 `git config user.name` 과 `user.email`
- 상시 실행: systemd service 등으로 위 명령을 등록
- 1회 실행 확인: `--run once --push false`

## 3. Data Format

Table 1. Fields of data/server.json

| Field | Meaning |
| --- | --- |
| `updated` | 수집 시각 (UTC, ISO 8601) |
| `cpu_pct` | CPU 사용률 (%), 1초 sampling |
| `mem_pct` | Memory 사용률 (%) |
| `disk_pct` | `--disk-path` 의 disk 사용률 (%) |
| `uptime_h` | 부팅 후 경과 시간 (hour) |

공개 repository이므로 host name, IP, process 이름은 기록하지 않는다. `updated` 가 오래되면 페이지는 서버가 응답하지 않는 것으로 표시한다.
