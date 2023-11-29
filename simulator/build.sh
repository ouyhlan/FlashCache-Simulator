#!/usr/bin/env bash

rm -rf ./bin


cmake -B bin -GNinja -DCMAKE_BUILD_TYPE=RelWithDebInfo -DCMAKE_EXPORT_COMPILE_COMMANDS=YES  .
#cmake -B bin -GNinja -DCMAKE_BUILD_TYPE=Debug  .
if [[ "$?" != 0  ]];then
	exit
fi

cmake --build bin 2>&1 | tee build.log


# https://stackoverflow.com/questions/22623045/return-value-of-redirected-bash-command
if [[ "${PIPESTATUS[0]}" != 0  ]];then
	cat ./bin.log | grep --color "error"
	exit
fi

exit