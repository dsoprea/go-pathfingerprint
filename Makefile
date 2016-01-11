export GOPATH=${PWD}

INSTALL_PATH=/usr/local/bin
BUILD_PACKAGES=pathfingerprint/pathfingerprint
PFINTERNAL_SOURCEFILES=src/pathfingerprint/pfinternal/*.go
PATHFINGERPRINT_SOURCEFILES=src/pathfingerprint/pathfingerprint/*.go
SOURCEFILES=${PFINTERNAL_SOURCEFILES} ${PATHFINGERPRINT_SOURCEFILES}
TARGET_BINARY_FILEPATH=${INSTALL_PATH}/pathfingerprint

all: bin/pathfingerprint

.PHONY: all clean install

clean:
	rm -f ${TARGET_BINARY_FILEPATH}

	rm -fr bin pkg 
	rm -fr src/gopkg.in src/code.google.com src/github.com

bin/pathfingerprint: ${SOURCEFILES}
	go get ${BUILD_PACKAGES}
	go install ${BUILD_PACKAGES}

install: bin/pathfingerprint
	install -m 755 bin/pathfingerprint ${TARGET_BINARY_FILEPATH}