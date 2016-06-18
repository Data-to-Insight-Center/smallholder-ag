display_help() {
    echo
    echo "Usage: $0 -c <config_file> --weekly|--daily [-day|-hour]" >&2
    echo
    echo "   -c, --config   Config file path. ex: conf/global_zambia.properties"
    echo "   -w, --weekly   Run the TextIt Ingest script weekly"
    echo "   -dw, --day     Day of the week the script should run on[0-6]"
    echo "   -d, --daily    Run the TextIt Ingest script daily"
    echo "   -hd, --hour    Hour of day the script should run on [0-23]"
    echo
}

while :
do
    case "$1" in
      -c | --config)    config_file="$2"
                        shift 2
                        ;;
      -h | --help)      display_help
                        exit 0
                        ;;
      -d | --daily)     daily="daily"
                        shift
                        ;;
      -hd | --hour)     hour="$2"
                        shift 2
                        ;;
      -w | --weekly)    weekly="weekly"
                        shift
                        ;;
      -dw | --day)      day="$2"
                        shift 2
                        ;;
      -*)               echo "Error: Unknown option: $1" >&2
                        exit 1
                        ;;
      *)                break
                        ;;
    esac
done

if [ "$config_file" == "" ]; then
    echo "Error: Please specify a configuration file path"
    display_help
    exit 1
fi

if [ "$daily" != "" ] && [ "$weekly" != "" ]; then
    echo "Error: Cannot specify both -w and -d options"
    display_help
    exit 1
fi

if [ "$daily" == "" ] && [ "$weekly" == "" ]; then
    echo "Error: Please specify either -w or -d option"
    display_help
    exit 1
fi

if [ "$hour" != "" ]; then
    if [[ ! "$hour" =~ ^[0-9]+$ ]] || [ "$hour" -gt 23 ]; then
        echo "Error: 'hour' should be between 0-23"
        display_help
        exit 1
    fi
else
    hour="0"
fi

if [ "$day" != "" ]; then
    if [[ ! "$day" =~ ^[0-6]+$ ]] || [ "$day" -gt 6 ]; then
        echo "Error: 'day' should be between 0-6"
        display_help
        exit 1
    fi
else
    day="0"
fi

this=$0
bin=`dirname "$this"`
bin=`cd "$bin"; pwd`
HOME=`cd "$bin"; cd ..; pwd`

start_script=$bin"/start-ingestor.sh"
log_file=$HOME"/cron_log.txt"
config_file_path=$HOME"/"$config_file

echo "Running "$0" script with "$config_file" configuration file"
if [ "$daily" != "" ]; then
    echo "The script is scheduled to run daily, at hour "$hour
fi
if [ "$weekly" != "" ]; then
    echo "The script is scheduled to run weekly, on day "$day
fi
echo "Log file location is "$log_file""

chmod +x $start_script

#write out current crontab
crontab -l > textitcron

#echo new cron into cron file
if [ "$daily" != "" ]; then
    echo "0 "$hour" * * * "$start_script" "$HOME" "$config_file" "$daily" >> "$log_file" 2>&1" >> textitcron
else
    echo "0 11 * * "$day" "$start_script" "$HOME" "$config_file" "$weekly" >> "$log_file" 2>&1" >> textitcron
fi

#install new cron file
crontab textitcron

rm textitcron
