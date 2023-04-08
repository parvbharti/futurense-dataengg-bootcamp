i="y"



echo " Enter first  no."

read num1

echo "Enter second no."

read num2

while [ $i = "y" ]
	do
		
		echo "1.Addition"

		echo "2.Subtraction"

		echo "3.Multiplication"

		echo "4.Division"

		echo "Enter your choice"

		read ch

		case $ch in

    			1)sum=`expr $num1 + $num2`

     			echo "Sum ="$sum;;

        		2)sum=`expr $num1 - $num2`

     			echo "Sub = "$sum;;

    			3)sum=`expr $num1 \* $num2`

     			echo "Mul = "$sum;;

    			4)sum=`echo "scale=2;$num1/$num2"|bc`

        		echo "div=" $sum;;

    			*)echo "Invalid choice";;

		esac

	echo "Do u want to continue ?[y/n]"

	read i

	if [ $i != "y" ]

	then

    	exit

fi    

done

