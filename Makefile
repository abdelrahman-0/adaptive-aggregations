.PHONY: all install build-debug build-release

BUILD_DIR=build-debug

all: build

install:
	./install_dependencies

build: build-debug build-release

build-debug:
	mkdir -p $(BUILD_DIR)
	cd $(BUILD_DIR) && cmake -DCMAKE_BUILD_TYPE=Debug .. && make && cd ..

build-release:
	mkdir -p $(BUILD_DIR)
	cd $(BUILD_DIR) && cmake -DCMAKE_BUILD_TYPE=Release .. && make && cd ..