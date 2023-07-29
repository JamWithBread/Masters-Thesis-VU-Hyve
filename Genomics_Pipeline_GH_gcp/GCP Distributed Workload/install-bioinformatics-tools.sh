#!/bin/bash

#install-bioinformatics-tools.sh

### Common Dependencies ###

sudo apt-get install autoconf automake make gcc perl zlib1g-dev libbz2-dev liblzma-dev libcurl4-gnutls-dev libssl-dev libncurses5-dev

### BCFTools ###

wget https://github.com/samtools/bcftools/releases/download/1.3.1/bcftools-1.3.1.tar.bz2 -O bcftools.tar.bz2
tar -xjvf bcftools.tar.bz2
cd bcftools-1.3.1
make
sudo make prefix=/usr/local/bin install
sudo ln -s /usr/local/bin/bin/bcftools /usr/bin/bcftools