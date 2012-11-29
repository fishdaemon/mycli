#!/bin/bash 
s3_bucket="dev.eu.millionmind.com"
s3_prefix="mysql/snapshots"

function cmd_backup()
{
	local _db=${1-$mysql_default_db}
	if [[ -z "$_db" ]]
	then
		echo "no db selected" 1>&2
	fi
	local _date=$(date -u --rfc-3339=ns | cut -f1 -d' ')
	local _time=$(date -u --rfc-3339=ns | cut -f2 -d' ')
	local _dir="$PWD/$_db/$_date"
	mkdir -p "$_dir"
	mysqldump -u$mysql_user -p$mysql_passwd -h$mysql_host $(__ignore_table $_db) $_db > "$_dir/$_time.sql" && s3cmd put "$_dir/$_time.sql" s3://$s3_bucket/$s3_prefix/$_db/$_date/$_time.sql && rm -r "$_db"
	

}
function cmd_config()
{
	
	if [[ ! -z "$2" && ! -z "$1" ]]
	then
		sed -i "s/$1=.*/$1='""${2}""'/" $HOME/.mycli
	elif [[ ! -z "$1" ]]
	then
		grep "$1" $HOME/.mycli | cut -d"'" -f2
	else
		cat $HOME/.mycli
	fi

}
function cmd_restore()
{
	local _db="${1-$mysql_default_db}"
	local _date=$(date -u --rfc-3339=ns | cut -f1 -d' ')
	local -i i len c
	local -i choice="${2-0}"
	c=1	
	local -a _dumps
	while read d
	do
		_dumps=("$d" "${_dumps[@]}")
	done <<<"$(s3cmd -H ls s3://$s3_bucket/$s3_prefix/$_db/$_date/*)"
	
	len=${#_dumps[@]}
	for (( i=0; i<$len; i++ ))
	do
		echo "$c : ${_dumps[$i]}"
		(( c++ ))
		[[ $c == 20 ]] && break
	done
	
	while [[ ! $choice =~ [1-9][0-9]* ]]
	do
		read -e -p"Pick a number to restore from: " choice
	done
	local sel="${_dumps[(($choice -1))]}" 
	echo "Downloading ${sel#*s3://}"
	local tmp=$(mktemp $PWD/dump.XXXXX)
	s3cmd --force get "s3://${sel#*s3://}" "$tmp"

	mysql -u$mysql_user -p$mysql_passwd -h$mysql_host $_db < "$tmp"
	rm "$tmp"


}
function __ignore_table()
{
	local _ret=""
	for t in ${mysql_ignored_tables}
	do 
		ret+="--ignore-table=$1.$t "
	done
	echo "$ret"
}
function cmd_setup()
{

	read -e -i "$aws_key" -p "AWS key: "  aws_key
	read -e -p "AWS secret: " -i "$aws_secret" aws_secret
	read -e -p "S3 bucket: " -i "$s3_bucket" s3_bucket
	read -e -p "S3 prefix: " -i "$s3_prefix" s3_prefix
	read -e -p "mysql user: "  -i "$mysql_user" mysql_user
	read -e -p "mysql password: " -i "$mysql_passwd" mysql_passwd
	read -e -p "mysql host: " -i "$mysql_host" mysql_host
	read -e -p "mysql defaultdb: " -i "$mysql_default_db" mysql_default_db
	read -e -p "mysql ignored tables: " -i "$mysql_ignored_tables" mysql_ignored_tables
	echo "# Make sure you qoute all values that include spaces" > "$HOME"/.mycli
	echo "aws_key='$aws_key'" >> "$HOME"/.mycli
	echo "aws_secret='$aws_secret'" >> "$HOME"/.mycli
	echo "s3_bucket='$s3_bucket'" >> "$HOME"/.mycli
	echo "s3_prefix='$s3_prefix'" >> "$HOME"/.mycli
	echo "mysql_user='$mysql_user'" >> "$HOME"/.mycli
	echo "mysql_passwd='$mysql_passwd'" >> "$HOME"/.mycli
	echo "mysql_host='$mysql_host'" >> "$HOME"/.mycli
	echo "mysql_default_db='$mysql_default_db'" >> "$HOME"/.mycli
	echo "mysql_ignored_tables='$mysql_ignored_tables'" >> "$HOME"/.mycli
	echo "__setup_done=true" >> "$HOME"/.mycli
	sudo apt-get install mysql-client s3cmd
	echo "access_key = $aws_key" >> "$HOME"/.s3cfg
	echo "secret_key = $aws_secret" >> "$HOME"/.s3cfg
	s3cmd --configure
}
function command_not_found_handle()
{
	echo "usage: $0 setup | backup [db_name] |restore [db_name] [n]"
}
[[ -f $HOME/.mycli ]] || cmd_setup 
. $HOME/.mycli
[[  $__setup_done ]] || echo "run $0 setup" 1>&2
c=$1
shift 1
cmd_$c "$@"