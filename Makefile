all: opcapi opc-cli

opcapi:
	export GOOS="windows"; export GOARCH="386"; go build github.com/konimarti/opc/cmds/opcapi

opc-cli:
	export GOOS="windows"; export GOARCH="386"; go build github.com/konimarti/opc/cmds/opc-cli
