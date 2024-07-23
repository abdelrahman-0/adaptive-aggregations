.PHONY: all install build-debug build-release

BUILD_DIR_DEBUG=build-debug
BUILD_DIR_RELEASE=build-release

all: build

install:
	./install_dependencies

build: build-debug build-release

build-debug:
	mkdir -p $(BUILD_DIR_DEBUG)
	cd $(BUILD_DIR_DEBUG) && cmake -DCMAKE_BUILD_TYPE=Debug .. && make && cd ..

build-release:
	mkdir -p $(BUILD_DIR_RELEASE)
	cd $(BUILD_DIR_RELEASE) && cmake -DCMAKE_BUILD_TYPE=Release .. && make && cd ..