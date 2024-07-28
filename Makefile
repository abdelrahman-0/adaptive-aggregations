.PHONY: all install build-debug build-release

BUILD_DIR_DEB=build-debug
BUILD_DIR_REL=build-release

TARGETS=ingress_singlethreaded_recv egress_singlethreaded_send ingress_multithreaded_recv egress_multithreaded_send
NETWORK_PAGE_SIZE_POWER=18

all: build

install:
	./install_dependencies

build: build-debug build-release

build-debug:
	mkdir -p $(BUILD_DIR_DEB) && cd $(BUILD_DIR_DEB) && cmake -DNETWORK_PAGE_SIZE_POWER=$(NETWORK_PAGE_SIZE_POWER) -DCMAKE_BUILD_TYPE=Debug .. && make $(TARGETS)

build-release:
	mkdir -p $(BUILD_DIR_REL) && cd $(BUILD_DIR_REL) && cmake -DNETWORK_PAGE_SIZE_POWER=$(NETWORK_PAGE_SIZE_POWER) -DCMAKE_BUILD_TYPE=Release .. && make $(TARGETS)
