echo "Extracting..."; 
echo "Downloading File..."; 
curl --output /Users/dongyanshen/Desktop/DYS/UoW_AI_Course/Data_Management/Mini_Project/web-server-access-log.txt.gz https://elasticbeanstalk-us-east-2-340729127361.s3.us-east-2.amazonaws.com/web-server-access-log.txt.gz; 
echo "Decompressing..."; 
gunzip /Users/dongyanshen/Desktop/DYS/UoW_AI_Course/Data_Management/Mini_Project/web-server-access-log.txt.gz > /Users/dongyanshen/Desktop/DYS/UoW_AI_Course/Data_Management/Mini_Project/web-server-access-log.txt;