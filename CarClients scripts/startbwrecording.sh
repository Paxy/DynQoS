rm -rf testings*
screen -dmS bw bwm-ng -u bits -t 500 -d 1 -o csv -F testingsCar.csv
