mkdir -p build && cd build 
cmake .. 
make install
mv /usr/local/lib64/libleveldb.a /usr/local/lib
