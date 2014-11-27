# coding: utf-8
require 'yaml'
require 'standalone_migrations'
StandaloneMigrations::Tasks.load_tasks

### standalone_migrations用のファイルやディレクトリを準備するタスク
desc "マイグレーションに必要なディレクトリとファイルを生成"
task :db_set do

  # コマンドライン引数を取得
  raise ArgumentError, "please set id!" unless ENV.member?('id')

  file_id = ENV['id']
  p "file_id: #{file_id}"

  # プロジェクトのホームパスを取得
  home_path = File.expand_path(File.dirname(__FILE__))
  home_path.slice!(/\/src$/)
  p "home_path: #{home_path}"

  #ログファイルの保存ディレクトリの指定
  log_path = home_path + "/log/"
  p "log_path: #{log_path}" if $DEBUG

  #ログ
  log = Logger.new("#{log_path}dbsetting_#{file_id}.log")
  log.level = Logger::INFO

  # config_crawlerファイルを読み込む
  config = YAML.load_file(home_path + "/config/config_crawler_#{file_id}.yml")
  p "config file loaded."
  log.info("/config/config_crawler_#{file_id}.yml loaded.")

  # DB設定を取得
  mysql_config = config["mysql_config"]
  p "mysql_config: #{mysql_config}"
  log.info("mysql_config: #{mysql_config}")

  ### 出力ファイル識別子に基づいてデータベースを準備
  # standalone_migrations用の設定ファイルを作成
  File.open("#{home_path}/.#{file_id}.standalone_migrations", "w") do |f|
    f.puts("db:")
    f.puts("  migrate: db/migrate/#{file_id}")
    f.puts("  schema: db/schema_#{file_id}.rb")
    f.puts("config:")
    f.puts("  database: db/config_#{file_id}.yml")
  end
  p ".#{file_id}.standalone_migrations created."
  log.info(".#{file_id}.standalone_migrations created.")

  # マイグレーション用設定ファイルを作成
  File.open("#{home_path}/db/config_#{file_id}.yml", "w") do |f|
    f.puts("development:")
    f.puts("  adapter: mysql2")
    f.puts("  encoding: utf8")
    f.puts("  reconnect: false")
    f.puts("  database: #{mysql_config["db_name"]}")
    f.puts("  pool: #{config["max_thread"]}")
    f.puts("  username: #{mysql_config["user"]}")
    f.puts("  password: #{mysql_config["password"]}")
    f.puts("  host: #{mysql_config["host"]}")
  end
  p "db/config_#{file_id}.yml created."
  log.info("db/config_#{file_id}.yml created.")

  # マイグレーションファイルを格納するディレクトリを作成
  Dir.mkdir("#{home_path}/db/migrate/#{file_id}") unless File.exist?("#{home_path}/db/migrate/#{file_id}")
  p "#{home_path}/db/migrate/#{file_id} made."
  log.info("#{home_path}/db/migrate/#{file_id} made.")

  # sampleディレクトリ内のmigrationファイルを作成したディレクトリへコピー
  files = Dir.glob("#{home_path}/db/migrate/sample/*")
  FileUtils.cp(files, "#{home_path}/db/migrate/#{file_id}")
  p "migration file copied to #{file_id}/ ."
  log.info("migration file copied to #{file_id}/ .")

end
