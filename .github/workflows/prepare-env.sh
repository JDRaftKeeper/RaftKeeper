#!/bin/bash

llvm_version=$1
llvm_priority=300

sudo apt install -y ninja-build ccache

# install llvm
#wget https://apt.llvm.org/llvm.sh
#chmod 755 llvm.sh && sudo ./llvm.sh ${llvm_version} all

# make clang as default compiler
#sudo update-alternatives --install /usr/bin/llvm-config llvm-config /usr/bin/llvm-config-${llvm_version} ${llvm_priority}
#sudo update-alternatives --install /usr/bin/llvm-symbolizer llvm-symbolizer /usr/bin/llvm-symbolizer-${llvm_version} ${llvm_priority}
sudo update-alternatives --install /usr/bin/clang clang /usr/bin/clang-${llvm_version} ${llvm_priority}
sudo update-alternatives --install /usr/bin/clang++ clang++ /usr/bin/clang++-${llvm_version} ${llvm_priority}

clang-${llvm_version} -v

# print clang complier runtime libs
complier_rt=`clang --print-libgcc-file-name --rtlib=compiler-rt`

if [ -f $complier_rt ]; then
  echo "exist complier_rt: ${complier_rt}"
fi

#echo "llvm-symbolizer version:"
#llvm-symbolizer --version
