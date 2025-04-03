all: opcapi opc-cli

opcapi:
	# 说明：golang 新版本 与 windows 老版本的兼容性是不被保证的，请找个老版本的 golang 镜像去编译 opcapi (比如 1.20.0)
	export CGO_ENABLED=0; export GOOS="windows"; export GOARCH="386"; go build github.com/konimarti/opc/cmds/opcapi

opc-cli:
	export GOOS="windows"; export GOARCH="386"; go build github.com/konimarti/opc/cmds/opc-cli

opc:
	go build github.com/konimarti/opc/cmds/opc
