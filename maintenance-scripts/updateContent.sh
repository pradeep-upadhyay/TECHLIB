#!/bin/bash -
#===============================================================================
#
#          FILE: updateContent.sh
#
#         USAGE: ./updateContent.sh
#
#   DESCRIPTION: 
#
#       OPTIONS: ---
#  REQUIREMENTS: ---
#          BUGS: ---
#         NOTES: ---
#        AUTHOR: Xu, Yue (ayxu), ayxu@amazon.com
#  ORGANIZATION: Economic Selling, OIH, SCOT
#       CREATED: 04/08/2018 18:20:28
#      REVISION:  ---
#===============================================================================

set -o nounset                                  # Treat unset variables as an error
set -x

root_dir=${PWD%/*}
filename="README.md"

cat /dev/null > ${root_dir}/${filename} && echo "# Contents" > ${root_dir}/${filename}

python3 ${PWD}/listFiles.py ${root_dir} >> ${root_dir}/${filename}
echo "Done!"
