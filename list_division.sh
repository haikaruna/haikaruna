######### list is a filename which contains n number of lines with each line having some values ########
######### Below program will create files with 10 values each depends on number of lines in the list file.
######### Example if the list file has contents from 1 to 25 on each line, then below program will create 3 files list_1,list_2,list_3 having values 1 to 10, 11 to 20 and 21 to 25 respectively.

h=$(cat list | wc -l)  ###Getting number of lines in the file

echo " Number of lines in list is $h"
list_num=$((h/10))    ####Finding number of batched to be created

echo "Number of files to be created $list_num"

if [ $list_num == 0 ]
then
    echo "Create only one list and start the wrapper script loop"
    cp list list_1
else
    echo "Batch division started"
    s=10
    e=10
    for i in $(seq 1 $list_num)
    do
      head -n $s list | tail -n $e > list_$i
      s=$((s + 10))
    done
    mim=$((h % 10))
    if [ "$mim" == 0 ]
    then
        echo "no need to add"
    else
        echo "Adding the remaining if any"
        rem=$((h % 10))
        echo "remaining objects are $rem"
        f=$((list_num + 1))
        #s=$((s + 10))
        head -n $s list | tail -n $rem > list_$f
        echo "Division completed number of batches created is $f"
    fi

fi
