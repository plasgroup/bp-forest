case "$PWD" in
    */bp-forest)
        break
    ;;
    *)
        echo "run this script in bp-forest directory"
        exit
    ;;
esac

BUILD_DIR="build"

OPTION="-S . -B $BUILD_DIR"

FLAGS="-O3"
FLAGS="$FLAGS -UPRINT_DEBUG=1"

FLAGS_HOST="-mcmodel=large -DHOST_MULTI_THREAD=32 $FLAGS"
FLAGS_HOST="$FLAGS_HOST -UMEASURE_XFER_BYTES"
FLAGS_HOST="$FLAGS_HOST -DRANK_ORIENTED_XFER"

FLAGS_DPU="$FLAGS"

cmake $OPTION -DCMAKE_CXX_FLAGS="$FLAGS_HOST" -DCMAKE_C_FLAGS="$FLAGS_DPU"
pushd $BUILD_DIR
make -j
popd
