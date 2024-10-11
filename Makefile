.PHONY: default install build-debug build-release

BUILD_DIR_DEB=build-debug
BUILD_DIR_REL=build-release
BUILD_DIR_RELWITHDEBINFO=build-relwithdebinfo

TARGETS=shuffle_homogeneous shuffle_heterogeneous groupby_homogeneous generate_data

default: build-release

install:
	./install_dependencies

build-debug:
	mkdir -p $(BUILD_DIR_DEB) && \
	cd $(BUILD_DIR_DEB) && \
	cmake -DCMAKE_BUILD_TYPE=Debug .. && \
	make $(TARGETS)

build-release:
	mkdir -p $(BUILD_DIR_REL) && \
	cd $(BUILD_DIR_REL) && \
	cmake -DCMAKE_BUILD_TYPE=Release .. && \
	make $(TARGETS)

build-relwithdebinfo:
	mkdir -p $(BUILD_DIR_RELWITHDEBINFO) && \
	cd $(BUILD_DIR_RELWITHDEBINFO) && \
	cmake -DCMAKE_BUILD_TYPE=RelWithDebInfo .. && \
	make $(TARGETS)