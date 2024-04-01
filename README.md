# Start local temporal cluster
doc: https://learn.temporal.io/getting_started/go/dev_environment/
command
```shell
temporal server start-dev
```

# Run the unit test
```shell
go test
```

# Start local temporal cluster
doc: https://learn.temporal.io/getting_started/go/hello_world_in_go/
command

# Trigger the workflow
```shell
# start the worker
go run worker/main.go

# start the workflow
go run start/main.go
```
