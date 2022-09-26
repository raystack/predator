#!/bin/bash

NAME="github.com/odpf/predator"

SYS=("linux" "darwin")
ARCH=("386" "amd64")
BUILD_DIR="dist"

build_executable() {
    EXECUTABLE_NAME=$1
    LD_FLAGS=$2
    for os in ${SYS[*]}; do
        for arch in ${ARCH[*]}; do
          if [ $os = "darwin" ] && [ $arch = "386" ]; then
            continue
            fi

            # create a folder named via the combination of os and arch
            TARGET="./$BUILD_DIR/$EXECUTABLE_NAME/${os}-${arch}"
            mkdir -p $TARGET

            # place the executable within that folder
            executable="${TARGET}/$EXECUTABLE_NAME"
            echo $executable
            GOOS=$os GOARCH=$arch go build -ldflags "$LD_FLAGS" -o $executable $NAME/cmd
        done
    done
}

build_executable predator
