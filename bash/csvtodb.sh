echo "Data Extraction Stage"
cut -d":" -f1,3,6 /etc/passwd >> /Users/dongyanshen/Desktop/DYS/UoW-AI\ Course/Data\ Management/extracted.txt
echo "Transformation"
tr ":" "," < /Users/dongyanshen/Desktop/DYS/UoW-AI\ Course/Data\ Management/extracted.txt

# echo "Loading"
# tr ":" "," < /Users/dongyanshen/Desktop/DYS/UoW-AI\ Course/Data\ Management/extracted.csv

