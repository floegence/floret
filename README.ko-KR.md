# Floret v3

v3의 공식 API, 코드 예제, 스토리지 SPI 및 마이그레이션 절차는 [README.md](README.md)에 있습니다. 이 문서는 오래된 API를 남기지 않기 위해 코드 예제를 중복하지 않습니다.

모듈 경로는 `github.com/floegence/floret/v3`입니다. v3는 승인된 Agent 수명 주기의 유일한 사실 원천입니다. 모든 모델 실행에 `provider.Gateway`만 사용하고, `runtime.Host`는 composition root에만 보관하며, Floret이 수명 주기 ID를 할당하고 ID에 바인딩된 좁은 핸들을 제공합니다. 시작 시 자동 마이그레이션이나 legacy decoder는 없습니다.
