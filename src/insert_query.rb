require 'open-uri'
require 'uri'
require 'mysql'
require 'rubygems'
require 'timeout'
require 'logger'
require 'yaml'

def insertData(source_file_name, my, log)

  begin

  count = 1
  puts "file: #{source_file_name}"
  log.info("[insertData] file: #{source_file_name}")

	# 1行ずつinsert文を実行していく処理
  File.open(source_file_name) do |file|
    while line = file.gets
      begin
        my.query(line)
      rescue => e
        p "[L#{count}]#{e.message}"
        log.error("[L#{count}]#{e.message}")
      ensure
        count = count + 1
      end
    end
  end

  rescue => e
    puts e.message
    log.error("[insertData]#{e.message}")
  end

end


def updateCreatedTime(table_name, yesterday_p1, exe_time, my, log)
  # createdの日時を変更する処理
  begin

    log.info("[updateCreatedTime] table: #{table_name}")

    sql = "update #{table_name} set created = #{yesterday_p1.strftime("%Y%m%d")}120000 where created between #{exe_time.strftime("%Y%m%d")}000000 and #{exe_time.strftime("%Y%m%d")}235959"
    my.query(sql)
  rescue => e
    p e.message
    log.error("[updateCreatedTime]#{e.message}")
  end
end




####################################
#            実行部分              #
####################################

raise ArgumentError, "please set file_id!!" if ARGV[0].nil?

p "***** insert query start *****" if $DEBUG

# ファイル識別子をコマンドライン引数から取得
file_id = ARGV[0]
p "file_id: #{file_id}" if $DEBUG

# プロジェクトのホームパスを取得
home_path = File.expand_path(File.dirname(__FILE__))
home_path.slice!(/\/src$/)
p "home_path: #{home_path}" if $DEBUG

#ログファイルの保存ディレクトリの指定
log_path = "#{home_path}/log/"
p "log_path: #{log_path}" if $DEBUG
# FileUtils.mkdir_p(log_path) unless File.exist?(log_path)

#sqlファイルの保存ディレクトリの指定
sql_path = "#{home_path}/sql/"
p "sql_path: #{sql_path}" if $DEBUG
# FileUtils.mkdir_p(sql_path) unless File.exist?(sql_path)

#実行開始時間(ログファイル名用)
exe_time = Time.now
p "exe_time: #{exe_time}" if $DEBUG
  
yesterday = exe_time - 60 * 60 * 24 * 2
yesterday_p1 = yesterday + 60 * 60 * 24 * 1

# 設定ファイルをロード
config = YAML.load_file("#{home_path}/src/config/config_crawler_#{file_id}.yml")
p "config_file loaded." if $DEBUG

#ログ
log = Logger.new("#{log_path}#{exe_time.strftime('%Y%m%d')}_insert_#{file_id}.log")
log.level = Logger::INFO

begin

  # 取り込むファイル名
  out_links = "#{sql_path}sqlpool_#{yesterday.strftime("%Y%m%d")}_#{file_id}.txt"
  out_links_sub = "#{sql_path}sqlpool_#{yesterday.strftime("%Y%m%d")}_#{file_id}_sub.txt"
  out_links_hash = "#{sql_path}sqlpool_hash_#{yesterday.strftime("%Y%m%d")}_#{file_id}.txt"
  out_links_hash_sub = "#{sql_path}sqlpool_hash_#{yesterday.strftime("%Y%m%d")}_#{file_id}_sub.txt"

  log.info("処理を開始")

  # DB名選択
  db_name = "crawler_#{file_id}"
  p "db: #{db_name}"
  log.info("db: #{db_name}")

  sleep(10)

  # 接続情報
  my = Mysql.new(config["mysql_config"]["host"], config["mysql_config"]["user"], config["mysql_config"]["password"], config["mysql_config"]["db_name"])
  my.charset = 'utf8'

  # データを挿入する処理
  insertData(out_links, my, log)
  # updateCreatedTime("out_links", yesterday_p1, exe_time, my, log)

  insertData(out_links_sub, my, log)
  # updateCreatedTime("out_links_sub", yesterday_p1, exe_time, my, log)

  insertData(out_links_hash, my, log)
  # updateCreatedTime("out_links_hash", yesterday_p1, exe_time, my, log)

  insertData(out_links_hash_sub, my, log)
  # updateCreatedTime("out_links_hash_sub", yesterday_p1, exe_time, my, log)

  log.info("処理が終了しました")

rescue => e
  log.error("[main]" + e.message)
end