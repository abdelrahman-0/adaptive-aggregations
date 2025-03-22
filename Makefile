.PHONY: default install all shuffle aggregation adaptive build

HT_PAGE_SIZE_POWER=16
NETWORK_PAGE_SIZE_POWER=17

BUILD_DIR_REL=build-release

TARGETS_SHUFFLE=shuffle_homogeneous shuffle_heterogeneous
TARGETS_AGGREGATION=aggregation_local aggregation_homogeneous aggregation_heterogeneous
TARGETS_ADAPTIVE=worker coordinator

default:
	@echo "No default target. Please choose an explicit target from the Makefile."

install:
	./install_deps.sh

all: TARGETS=$(TARGETS_SHUFFLE) $(TARGETS_AGGREGATION) $(TARGETS_ADAPTIVE)
all: build

shuffle: TARGETS=$(TARGETS_SHUFFLE)
shuffle: build

aggregation: TARGETS=$(TARGETS_AGGREGATION)
aggregation: build

adaptive: TARGETS=$(TARGETS_ADAPTIVE)
adaptive: build

build:
	mkdir -p $(BUILD_DIR_REL) && \
	cd $(BUILD_DIR_REL) && \
	cmake -DNETWORK_PAGE_SIZE_POWER=$(NETWORK_PAGE_SIZE_POWER) -DHT_PAGE_SIZE_POWER=$(HT_PAGE_SIZE_POWER) -DCMAKE_BUILD_TYPE=Release .. && \
	make $(TARGETS)
