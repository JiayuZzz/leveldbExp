mkdir -p build && cd build 
cmake .. 
make install -j32
mv /usr/local/lib64/libleveldb.a /usr/local/lib
