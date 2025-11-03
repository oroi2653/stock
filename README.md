이 프로젝트는 KOSPI/KOSDAQ 전 종목(2000+)을 매일 KST 08:00/16:00에 스캔하여 텔레그램으로 알림을 보내는 스캐너입니다.

설정
1) 텔레그램
   - @BotFather로 봇 생성 → 토큰 획득
   - 봇과 대화창에서 /start → https://api.telegram.org/bot<TOKEN>/getUpdates 로 chat_id 확인
   - GitHub 저장소 Settings → Secrets and variables → Actions:
     - TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID 등록

2) 설치
   - 저장소 루트에 config.yaml가 있어야 합니다(본 파일 참조). 민감정보는 비워두고 Secrets 사용.
   - requirements.txt, app/*, .github/workflows/scan.yml 그대로 사용

3) 로컬 테스트(선택)
   - python -m venv .venv && source .venv/bin/activate
   - pip install -r requirements.txt
   - export TELEGRAM_BOT_TOKEN=...; export TELEGRAM_CHAT_ID=...
   - python -m app.main

4) GitHub Actions(자동)
   - 스케줄: KST 08:00 / 16:00
   - 워크플로에서 4샤드 병렬로 스캔(TOT_SHARDS=4)
   - CHUNK_SIZE/PAUSE, TOT_SHARDS는 워크플로 env에서 조정 가능

메시지 포맷
- 헤더: 한국시간 + 최다 신호
  🔔 스캔: 2025-11-03 10:53 KST
  신호 : 🔴 전화선 골크 콤보
- 종목:
  에이치케이 (044780) | O: 1403.00 C: 1420.00
  발견된 시점의 차트

주의
- 대량 전송 방지를 위해 scan.max_alerts_per_run로 상한을 조정하세요.
- 일부 상폐/정지 종목은 데이터가 비어 스킵될 수 있습니다.
- 투자는 본인 책임이며, 이 코드는 정보 제공 목적입니다.# stock
