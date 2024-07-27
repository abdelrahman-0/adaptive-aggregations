.PHONY: all install build-debug build-release

BUILD_DIR_DEBUG=build-debug
BUILD_DIR_RELEASE=build-release

all: build

install:
	./install_dependencies

build: build-debug build-release

build-debug:
	mkdir -p $(BUILD_DIR_DEBUG) && cd $(BUILD_DIR_DEBUG) && cmake -DCMAKE_BUILD_TYPE=Debug .. && make stream_egress stream_ingress epoll_ingress multithreaded_ingress stream_egress_send stream_ingress_multishot stream_ingress_multishot_multithreaded

build-release:
	mkdir -p $(BUILD_DIR_RELEASE) && cd $(BUILD_DIR_RELEASE) && cmake -DCMAKE_BUILD_TYPE=Debug .. && make stream_egress stream_ingress epoll_ingress multithreaded_ingress stream_egress_send stream_ingress_multishot stream_ingress_multishot_multithreaded
