this=$0
bin=`dirname "$this"`
bin=`cd "$bin"; pwd`
HOME=`cd "$bin"; cd ..; pwd`

start_script=$bin"/start-downloader.sh"
log_file=$HOME"/cron_log.txt"
config_file=$HOME"/"$1

echo "Start script: "$start_script
echo "Config file: "$config_file
echo "Cron log file: "$log_file

chmod +x $start_script

#write out current crontab
crontab -l > textitcron
#echo new cron into cron file
#echo "*/1 * * * * "$start_script" "$HOME" "$1" d >> "$log_file" 2>&1" >> textitcron
echo "*/1 * * * * "$start_script" "$HOME" "$1" >> "$log_file" 2>&1" >> textitcron
#install new cron file
crontab textitcron
rm textitcron