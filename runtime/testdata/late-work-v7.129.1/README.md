# Late host-work receipt

Synthetic SQLite generated with the published `github.com/floegence/floret/v7@v7.129.1`, commit `47f4dc7b3f98104406e46bd8426693ea30ec3a8d`, module sum `h1:wPm1QNmvq5jJ1agNweTH/+7pUGFhhINjiStCbKzOWV0=`. The runtime test pins the SHA-256 of the frozen database. No production data or external commands.

In a blank Go module, copy `producer_test.go.txt` to `case_test.go`, require that exact release, run `GOWORK=off go mod tidy`, then `AB_EARLY=0 AB_SEED="$PWD/new-seed" GOWORK=off go test -count=1 -run '^TestWriteHistory$'`. Timestamps vary; do not regenerate historical expectations from the reader under test.
