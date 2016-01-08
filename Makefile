export GOPATH=${PWD}

INSTALL_PATH=/usr/local/bin
BUILD_PACKAGES=pathfingerprint/pfmain
PFINTERNAL_SOURCEFILES=src/pathfingerprint/pfinternal/*.go
PFMAIN_SOURCEFILES=src/pathfingerprint/pfmain/*.go
SOURCEFILES=${PFINTERNAL_SOURCEFILES} ${PFMAIN_SOURCEFILES}
TARGET_BINARY_FILEPATH=${INSTALL_PATH}/pathfingerprint

all: bin/pfmain

.PHONY: all clean install

clean:
	rm -f ${TARGET_BINARY_FILEPATH}

	rm -fr bin pkg 
	rm -fr src/gopkg.in src/code.google.com src/github.com

bin/pfmain: ${SOURCEFILES}
	go get ${BUILD_PACKAGES}
	go install ${BUILD_PACKAGES}

install: bin/pfmain
	install -m 755 bin/pfmain ${TARGET_BINARY_FILEPATH}