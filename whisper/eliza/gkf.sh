#!/bin/bash
vmlinux="./vmlinux"
rm pm_pc*
while read -r line
do
    echo $line
    access=`echo $line | cut -c 1`
    case $access in
        U) 
            pc=`echo $line | cut -d " " -f 6`
            echo $pc >> pm_pc.txt;;
        W|R) 
            pc=`echo $line | cut -d " " -f 7`
            echo $pc >> pm_pc.txt;;
            # func=`addr2line -e $vmlinux -psaiCf $pc`
            # stmt=$access" "$func
            # echo $stmt;;
        M)
            # echo $line;;
    esac
done < /dev/stdin

echo "Writing unique PCs"
sort pm_pc.txt | uniq | addr2line -e $vmlinux -psCiaf > kf_mmiotrace.txt
