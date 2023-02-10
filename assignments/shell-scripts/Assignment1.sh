#Hashbang/ Shebang

#i/bin/sh

#Print the greeting message
echo "Enter City Name 1"
read Name1
echo "Enter city Name 2"
read Name2
echo "Enter city Name 3"
read Name3
touch cities.txt
echo $Name1 >> cities.txt
echo $Name2 >> cities.txt
echo $Name3 >> cities.txt
echo "Cities Details"
cat cities.txt
sed -i 's/New/Old/gi' cities.txt
cat cities.txt| grep "Old" >> filter_cities.txt
echo "New Cities Details"
cat filter_cities.txt
