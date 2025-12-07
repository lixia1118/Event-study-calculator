import pandas as pd
import sqlite3
import logging
import os
import shutil
from tqdm import tqdm

# 本代码的目的是将日个股回报率数据导入到SQLite数据库中，以便后续进行事件研究
# 代码的输入文件路径是 '公司数据集/日个股回报率/TRD_Daily_20150101-20250101.csv'，从CSMAR数据库获取
# 代码的输出文件路径是 'Event study_data/stock_daily_data.db'

print("当前工作目录 : %s" % os.getcwd())

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('Event study_code/import to sqlite/import_to_sqlite.log'),
        logging.StreamHandler()
    ]
)

def ensure_directory_exists(file_path):
    """确保文件所在目录存在"""
    directory = os.path.dirname(file_path)
    if not os.path.exists(directory):
        os.makedirs(directory)
        logging.info(f"创建目录: {directory}")

def create_database():
    """创建SQLite数据库和表"""
    try:
        db_path = 'Event study_data/stock_daily_data.db'
        
        # 如果数据库已存在，则删除
        if os.path.exists(db_path):
            try:
                os.remove(db_path)
                logging.info(f"已删除旧数据库: {db_path}")
            except Exception as e:
                logging.warning(f"删除旧数据库失败: {e}")
                
        ensure_directory_exists(db_path)
        
        # 连接到数据库（如果不存在则创建）
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # 创建表
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS daily_data (
            Stkcd TEXT,                    -- 证券代码
            Trddt TEXT,                    -- 交易日期
            Opnprc REAL,                   -- 日开盘价
            Hiprc REAL,                    -- 日最高价
            Loprc REAL,                    -- 日最低价
            Clsprc REAL,                   -- 日收盘价
            Dnshrtrd REAL,                 -- 日个股交易股数
            Dnvaltrd REAL,                 -- 日个股交易金额
            Dsmvosd REAL,                  -- 日个股流通市值
            Dsmvtll REAL,                  -- 日个股总市值
            Dretwd REAL,                   -- 考虑现金红利再投资的日个股回报率
            Dretnd REAL,                   -- 不考虑现金红利的日个股回报率
            Adjprcwd REAL,                 -- 考虑现金红利再投资的收盘价的可比价格
            Adjprcnd REAL,                 -- 不考虑现金红利的收盘价的可比价格
            Markettype INTEGER,            -- 市场类型
            Capchgdt TEXT,                 -- 最新股本变动日期
            Trdsta INTEGER,                -- 交易状态
            Ahshrtrd_D REAL,               -- 日盘后成交总量
            Ahvaltrd_D REAL,               -- 日盘后成交总额
            PreClosePrice REAL,            -- 昨收盘(交易所)
            ChangeRatio REAL,              -- 涨跌幅
            LimitDown REAL,                -- 跌停价
            LimitUp REAL,                  -- 涨停价
            LimitStatus INTEGER,           -- 涨跌停状态
            PRIMARY KEY (Stkcd, Trddt)
        )
        ''')
        
        conn.commit()
        conn.close()
        logging.info("数据库和表创建成功")
        
    except Exception as e:
        logging.error(f"创建数据库时发生错误: {str(e)}")
        raise

def insert_or_ignore(table, conn, keys, data_iter):
    """
    使用 INSERT OR IGNORE 插入数据，忽略主键冲突
    """
    sql = "INSERT OR IGNORE INTO {table} ({keys}) VALUES ({values})".format(
        table=table.name,
        keys=", ".join(keys),
        values=", ".join(["?"] * len(keys))
    )
    conn.executemany(sql, data_iter)

def import_data():
    """将CSV数据导入数据库"""
    try:
        db_path = 'Event study_data/stock_daily_data.db'
        csv_path = '公司数据集/日个股回报率/TRD_Daily_20150101-20250101.csv'
        
        # 检查文件是否存在
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV文件不存在: {csv_path}")
            
        # 检查磁盘空间
        disk_usage = shutil.disk_usage(os.path.dirname(db_path))
        free_space_gb = disk_usage.free / (1024 * 1024 * 1024)  # 转换为GB
        if free_space_gb < 1:  # 小于1GB
            logging.warning(f"磁盘空间可能不足，当前可用空间: {free_space_gb:.2f}GB")
        
        # 连接到数据库
        conn = sqlite3.connect(db_path)
        
        # 分块读取CSV文件，每次读取20万行
        chunk_size = 200000
        
        # 获取总行数用于进度条
        total_rows = sum(1 for _ in open(csv_path, 'r', encoding='utf-8')) - 1  # 减去标题行
        
        with tqdm(total=total_rows, desc="导入数据") as pbar:
            for chunk in pd.read_csv(csv_path, chunksize=chunk_size):
                try:
                    # 将数据写入数据库
                    chunk.to_sql('daily_data', conn, if_exists='append', index=False, method=insert_or_ignore)
                    conn.commit()  # 每次写入后提交
                    pbar.update(len(chunk))
                except Exception as chunk_error:
                    logging.error(f"处理数据块时发生错误: {str(chunk_error)}")
                    conn.rollback()  # 发生错误时回滚
                    continue
            
        conn.close()
        logging.info("数据导入完成")
        
    except Exception as e:
        logging.error(f"导入数据时发生错误: {str(e)}")
        raise

def main():
    try:
        create_database()
        import_data()
    except Exception as e:
        logging.error(f"程序执行过程中发生错误: {str(e)}")
        raise

if __name__ == '__main__':
    main()