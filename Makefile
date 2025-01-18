.PHONY: default install build-debug build-release

BUILD_DIR_DEB=build-debug
BUILD_DIR_REL=build-release
BUILD_DIR_RELWITHDEBINFO=build-relwithdebinfo
HT_PAGE_SIZE_POWER=16
NETWORK_PAGE_SIZE_POWER=17

TARGETS=aggregation_local aggregation_homogeneous #worker_homogeneous coordinator
# aggregation_homogeneous aggregation_heterogeneous shuffle_homogeneous shuffle_heterogeneous

default: build-release

install:
	./install_dependencies.sh

build-debug:
	mkdir -p $(BUILD_DIR_DEB) && \
	cd $(BUILD_DIR_DEB) && \
	cmake -DNETWORK_PAGE_SIZE_POWER=$(NETWORK_PAGE_SIZE_POWER) -DHT_PAGE_SIZE_POWER=$(HT_PAGE_SIZE_POWER) -DCMAKE_BUILD_TYPE=Debug .. && \
	make $(TARGETS)

build-release:
	mkdir -p $(BUILD_DIR_REL) && \
	cd $(BUILD_DIR_REL) && \
	cmake -DNETWORK_PAGE_SIZE_POWER=$(NETWORK_PAGE_SIZE_POWER) -DHT_PAGE_SIZE_POWER=$(HT_PAGE_SIZE_POWER) -DCMAKE_BUILD_TYPE=Release .. && \
	make $(TARGETS)

build-relwithdebinfo:
	mkdir -p $(BUILD_DIR_RELWITHDEBINFO) && \
	cd $(BUILD_DIR_RELWITHDEBINFO) && \
	cmake -DNETWORK_PAGE_SIZE_POWER=$(NETWORK_PAGE_SIZE_POWER) -DHT_PAGE_SIZE_POWER=$(HT_PAGE_SIZE_POWER) -DCMAKE_BUILD_TYPE=RelWithDebInfo .. && \
	make $(TARGETS)
