# Floret v7

v7의 공식 API, 코드 예제, 스토리지 SPI 및 마이그레이션 절차는 [README.md](README.md)에 있습니다. 이 문서는 오래된 API를 남기지 않기 위해 코드 예제를 중복하지 않습니다.

모듈 경로는 `github.com/floegence/floret/v7`입니다. v7는 승인된 Agent 수명 주기의 유일한 사실 원천입니다. 모든 모델 실행에 `provider.Gateway`만 사용하고, `runtime.Host`를 composition root에 보관하며, typed `ThreadService`를 제공합니다. `runtime.Open`은 Floret이 소유한 지원 대상 도메인 상태를 자동으로 마이그레이션합니다.
