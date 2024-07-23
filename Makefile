.PHONY: all build install

BUILD_DIR=build_debug

all: build

install:
	./install_dependencies

build:
	mkdir -p $(BUILD_DIR)
	cd $(BUILD_DIR) && cmake -DCMAKE_BUILD_TYPE=Debug .. && make && cd ..