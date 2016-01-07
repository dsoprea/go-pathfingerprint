GOPATH=${PWD}
INSTALL_PATH=/usr/local/bin
PACKAGE_PATH=src/pathfingerprint

PFMAIN_PATH=${PACKAGE_PATH}/pfmain
PFINTERNAL_PATH=${PACKAGE_PATH}/pfinternal

all: pathfingerprint

clean:
	rm -fr pkg/* bin/* 
	rm -fr src/code.google.com src/github.com src/gopkg.in

	rm -f ${INSTALL_PATH}/pathfingerprint

pathfingerprint: ${PFMAIN_PATH}/*.go ${PFINTERNAL_PATH}/*.go
	go get pathfingerprint/pfmain
	go build pathfingerprint/pfmain

	rm pfmain

install: pathfingerprint
	install -m 755 bin/pfmain ${INSTALL_PATH}/pathfingerprint
