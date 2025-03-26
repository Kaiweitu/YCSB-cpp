rm -rf build && mkdir build && cd build
cmake -DBIND_CACHELIB=ON \
      -DCMAKE_PREFIX_PATH=/home/kaiwei/CacheLib/opt/cachelib \
      -DBIND_ROCKSDB=OFF \
      -DBIND_LMDB=OFF \
      -DBIND_LEVELDB=OFF \
      ..

make clean && make -j