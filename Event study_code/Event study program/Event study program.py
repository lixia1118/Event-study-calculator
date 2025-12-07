import pandas as pd
import statsmodels.api as sm
from scipy.stats import ttest_1samp
import logging
from typing import Tuple, Optional, Dict, Any
import os
import sqlite3
import numpy as np
import warnings
import multiprocessing
from functools import partial
from tqdm import tqdm

# 该脚本用于计算上市公司在给定事件日的股票异常收益率以及累计异常收益率，估计窗口为自事件日前8天开始倒推120天，即[-128,-9]，事件窗口为事件日前3天后5天，即[-3,5]，大盘指数为沪深300。以上参数皆可自己调整


warnings.filterwarnings('ignore')

# 配置日志
log_file = 'Event study_code/Event study program/event_study.log'
# 确保日志目录存在
os.makedirs(os.path.dirname(log_file), exist_ok=True)

# 创建文件处理器，指定编码为utf-8
file_handler = logging.FileHandler(log_file, encoding='utf-8')
stream_handler = logging.StreamHandler()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[file_handler, stream_handler]
)

class EventStudy:
    def __init__(self, event_window_before: int = 3, event_window_after: int = 5, 
                 estimation_window_length: int = 120, estimation_window_gap: int = 5):
        """
        初始化事件研究参数
        
        Args:
            event_window_before: 事件日前天数
            event_window_after: 事件日后天数
            estimation_window_length: 估计窗口长度（交易日）
            estimation_window_gap: 估计窗口结束日与事件窗口开始日之间的间隔天数
        """
        self.event_window_before = event_window_before
        self.event_window_after = event_window_after
        self.estimation_window_length = estimation_window_length
        self.estimation_window_gap = estimation_window_gap
        
        # 验证参数
        self._validate_parameters()
        
        # 定义指数代码映射
        self.index_mapping = {
            '000300': '沪深300指数',
            '000001': '上证综合指数',
            '399106': '深证综合指数'
        }
        
    def _validate_parameters(self):
        """验证参数的有效性"""
        if self.event_window_before < 0 or self.event_window_after < 0:
            raise ValueError("事件窗口天数必须为非负数")
        if self.estimation_window_length <= 0:
            raise ValueError("估计窗口长度必须为正数")
        if self.estimation_window_gap < 0:
            raise ValueError("估计窗口间隔天数必须为非负数")
    
    def _get_company_market(self, company_code: str) -> Optional[str]:
        """
        根据公司代码判断所属市场
        
        Args:
            company_code: 公司代码
            
        Returns:
            'sh' - 沪市
            'sz' - 深市
            None - 跳过该股票(以8或9开头)
        """
        # 将公司代码补全为6位
        full_code = str(company_code).zfill(6)
        
        # 跳过以8或9开头的股票代码
        if full_code.startswith(('8', '9')):
            # logging.info(f"跳过以8或9开头的股票代码: {company_code}")
            return None
        
        # 沪市主板: 600, 601, 603, 605, 688, 689开头
        if full_code.startswith(('600', '601', '603', '605', '688', '689')):
            return 'sh'
        # 深市主板: 000, 001, 002, 003, 300, 301开头
        elif full_code.startswith(('000', '001', '002', '003', '300', '301')):
            return 'sz'
        else:
            # logging.warning(f"无法识别的公司代码: {company_code}, 默认使用沪深300指数")
            return 'hs300'
    
    def _get_index_code(self, company_code: str, custom_index: Optional[str] = None) -> Optional[str]:
        """
        根据公司代码获取对应的指数代码
        
        Args:
            company_code: 公司代码
            custom_index: 用户指定的指数代码(可选)
            
        Returns:
            指数代码或None(如果跳过该股票)
        """
        if custom_index and custom_index in self.index_mapping:
            return custom_index
        
        market = self._get_company_market(company_code)
        if market is None:
            return None
        
        if market == 'sh':
            return '000001'  # 上证综合指数
        elif market == 'sz':
            return '399106'  # 深证综合指数
        else:
            return '000300'  # 沪深300指数
    
    def get_data(self, company_code: str, event_date: str, custom_index: Optional[str] = None) -> Optional[pd.DataFrame]:
        """
        从数据库获取股票和指数数据
        
        Args:
            company_code: 公司代码
            event_date: 事件日期
            custom_index: 用户指定的指数代码(可选)
            
        Returns:
            合并后的数据DataFrame或None（如果获取失败或跳过该股票）
        """
        try:
            # 1. 确定使用的指数代码
            index_code = self._get_index_code(company_code, custom_index)
            if index_code is None:
                # logging.info(f"跳过公司 {company_code} 的数据获取")
                return None
                
            index_name = self.index_mapping.get(index_code, f"未知指数({index_code})")
            # logging.info(f"公司 {company_code} 使用指数: {index_name}")
            
            # 2. 读取指数数据
            index_file = '公司数据集/指数文件1990-12-19 - 2025-04-24/TRD_Index.xlsx'
            if not os.path.exists(index_file):
                logging.error("指数数据文件不存在")
                return None
                
            index_data = pd.read_excel(index_file)
            index_data = index_data[index_data['Indexcd'] == index_code]
            if index_data.empty:
                logging.warning(f"未找到指数 {index_name} 的数据")
                return None
                
            # 3. 读取股票数据
            conn = sqlite3.connect('Event study_data/stock_daily_data.db')
            query = f"SELECT Trddt, Dretnd FROM daily_data WHERE Stkcd = '{company_code}'"
            stock_data = pd.read_sql_query(query, conn)
            conn.close()
            
            if stock_data.empty:
                # logging.warning(f"未找到公司 {company_code} 的股票数据")
                return None
                
            # 4. 统一日期格式
            index_data['date'] = pd.to_datetime(index_data['Trddt'])
            stock_data['date'] = pd.to_datetime(stock_data['Trddt'])
            event_date = pd.to_datetime(event_date)
            
            # 5. 重命名列
            index_data = index_data.rename(columns={'Retindex': 'index_return'})
            stock_data = stock_data.rename(columns={'Dretnd': 'stock_return'})
            
            # 6. 找到事件日最近的交易日
            # 首先检查事件日是否为交易日
            event_date_only = event_date.date()
            index_data_date_only = index_data['date'].dt.date
            is_trading_day = (index_data_date_only == event_date_only).any()
            
            if is_trading_day:
                # 如果事件日是交易日，直接使用
                closest_date = event_date
            else:
                # 如果事件日不是交易日，找到事件日之后最近的交易日
                future_dates = index_data[index_data['date'] > event_date]
                if not future_dates.empty:
                    closest_date = future_dates['date'].min()
                    logging.info(f"事件日 {event_date} 不是交易日，使用之后最近的交易日 {closest_date} 作为0点")
                else:
                    # 如果之后没有交易日，则使用最近的交易日（可能是之前的）
                    date_diffs = (index_data['date'] - event_date).abs()
                    closest_idx = date_diffs.idxmin()
                    closest_date = index_data.loc[closest_idx, 'date']
                    logging.warning(f"事件日 {event_date} 不是交易日，且之后没有交易日，使用最近的交易日 {closest_date}")
            
            # 7. 计算需要的数据范围
            # 获取所有交易日并按日期排序
            trading_days = index_data['date'].sort_values().reset_index(drop=True)
            
            # 找到事件日最近的交易日在所有交易日中的位置
            matching_days = trading_days[trading_days == closest_date]
            if matching_days.empty:
                logging.warning(f"未找到交易日 {closest_date}")
                return None
                
            event_idx = matching_days.index[0]
            
            # 计算需要的前后交易日数量
            total_trading_days = (self.estimation_window_length + self.estimation_window_gap + 
                                 self.event_window_before + self.event_window_after+1)
            
            # 计算开始和结束的索引
            # 确保有足够的数据用于估计窗口
            estimated_start_idx = event_idx - self.estimation_window_length - self.estimation_window_gap - self.event_window_before
            start_idx = max(0, estimated_start_idx)
            end_idx = min(len(trading_days), event_idx + self.event_window_after + 1)
            
            # 确保索引有效
            if start_idx >= len(trading_days) or end_idx <= 0:
                logging.warning("计算出的日期范围无效")
                return None
                
            # 获取实际的开始和结束日期
            start_date = trading_days.iloc[start_idx]
            end_date = trading_days.iloc[end_idx - 1]  # -1是因为切片是左闭右开
            
            # 8. 筛选数据
            index_data = index_data[(index_data['date'] >= start_date) & 
                                  (index_data['date'] <= end_date)]
            stock_data = stock_data[(stock_data['date'] >= start_date) & 
                                  (stock_data['date'] <= end_date)]
            
            if index_data.empty or stock_data.empty:
                # logging.warning("筛选后的数据为空")
                return None
            
            # 9. 合并数据
            merged_data = pd.merge(stock_data, index_data, on='date')
            if merged_data.empty:
                # logging.warning(f"公司 {company_code} 和指数的数据没有重叠日期")
                return None
            
            # 10. 数据清理
            merged_data['stock_return'] = pd.to_numeric(merged_data['stock_return'], errors='coerce')
            merged_data['index_return'] = pd.to_numeric(merged_data['index_return'], errors='coerce')
            merged_data = merged_data.dropna(subset=['stock_return', 'index_return'])
            
            # 11. 记录日志
            logging.info("-----------------------------------------")
            logging.info(f"事件日是: {event_date}，事件日最近的交易日是: {closest_date}")
            logging.info(f"数据日期范围: {merged_data['date'].min()} 到 {merged_data['date'].max()}")
            logging.info(f"实际交易日数量: {len(merged_data)}")
            logging.info(f"使用的指数: {index_name}")
            
            
            return merged_data[['date', 'stock_return', 'index_return']]
            
        except Exception as e:
            logging.error(f"获取数据时发生错误: {str(e)}")
            return None

    def extract_event_data(self, event_date: str, data_frame: pd.DataFrame) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame], int]:
        """
        提取估计期和事件窗口数据
        
        Args:
            event_date: 事件日期
            data_frame: 包含股票和指数数据的DataFrame
            
        Returns:
            估计期数据和事件窗口数据的元组
        """
        try:
            event_date = pd.to_datetime(event_date)
            data_frame = data_frame.reset_index(drop=True)
            
            # 找到事件日
            # 只比较日期部分，忽略时间
            data_frame['date_only'] = data_frame['date'].dt.date
            event_date_only = event_date.date()
            
            # 找到最接近事件日的交易日
            date_diffs = (data_frame['date_only'] - event_date_only).abs()
            event_idx = date_diffs.idxmin()
            closest_date = data_frame.iloc[event_idx]['date_only']
            
            # 检查事件日是否为交易日
            is_trading_day = closest_date == event_date_only
            
            # 如果事件日不是交易日，找到事件日之后最近的交易日
            if not is_trading_day:
                # 找到事件日之后的所有交易日
                future_mask = data_frame['date_only'] > event_date_only
                future_indices = data_frame[future_mask].index
                if len(future_indices) > 0:
                    # 使用事件日之后最近的交易日作为0点
                    event_idx = future_indices[0]
                    closest_date = data_frame.iloc[event_idx]['date_only']
                    logging.info(f"事件日 {event_date_only} 不是交易日，使用之后最近的交易日 {closest_date} 作为0点")
                else:
                    logging.warning(f"事件日 {event_date_only} 不是交易日，且之后没有交易日")
            
            # if is_trading_day:
            #     logging.info(f"事件日 {event_date_only} 是交易日")
            # else:
            #     logging.info(f"事件日 {event_date_only} 不是交易日，最近的交易日是 {closest_date}")
            
            
            # 计算预期的事件窗口长度
            expected_length = self.event_window_before + self.event_window_after + 1
            
            # 提取事件窗口数据：前N天 + 0点交易日 + 后N天
            start_idx = event_idx - self.event_window_before
            end_idx = event_idx + self.event_window_after
            
            # 确保索引在有效范围内
            event_start_idx = max(0, start_idx)
            event_end_idx = min(len(data_frame), end_idx + 1)  # +1是因为Python切片是左闭右开
            
            # 提取事件窗口数据
            event_data = data_frame.iloc[event_start_idx:event_end_idx].copy()
            
            # 重新计算事件日在事件窗口中的位置
            event_idx_in_window = event_idx - event_start_idx
            
            # 检查是否有足够的数据
            if len(event_data) < expected_length:
                logging.warning(f"数据不足，无法提取完整的{expected_length}天事件窗口，实际提取{len(event_data)}天")
                pass
            
            # 提取估计期数据
            # 计算估计窗口的开始和结束索引
            estimation_end_idx = event_start_idx - self.estimation_window_gap
            estimation_start_idx = estimation_end_idx - self.estimation_window_length
            
            # 确保索引在有效范围内
            estimation_start_idx = max(0, estimation_start_idx)
            estimation_end_idx = min(len(data_frame), estimation_end_idx)
            
            # 计算实际的估计窗口长度
            actual_estimation_length = estimation_end_idx - estimation_start_idx
            
            # 如果估计窗口长度不足，尝试向后扩展估计窗口
            # 这将确保我们尽可能接近期望的窗口长度
            if actual_estimation_length < self.estimation_window_length and estimation_start_idx == 0:
                # 如果已经到了数据开头，尝试向后扩展
                additional_days_needed = self.estimation_window_length - actual_estimation_length
                estimation_end_idx = min(estimation_end_idx + additional_days_needed, len(data_frame))
                actual_estimation_length = estimation_end_idx - estimation_start_idx
                logging.info(f"估计窗口已调整: 从{actual_estimation_length - additional_days_needed}天扩展到{actual_estimation_length}天")
            elif actual_estimation_length < self.estimation_window_length:
                logging.warning(f"估计窗口长度不足: 期望{self.estimation_window_length}天，实际{actual_estimation_length}天")
            
            estimation_data = data_frame.iloc[estimation_start_idx:estimation_end_idx].copy()
            
            return estimation_data, event_data, event_idx_in_window
            
        except Exception as e:
            logging.error(f"提取事件数据时发生错误: {str(e)}")
            return None, None, -1
            
    def calculate_regression_coefficients(self, estimation_data: pd.DataFrame) -> Tuple[float, float]:
        """
        使用估计期数据进行OLS回归，计算回归参数
        
        Args:
            estimation_data: 估计期数据
            
        Returns:
            alpha和beta的元组
        """
        try:
            # 确保数据类型正确
            X = np.array(estimation_data['index_return'], dtype=float)
            y = np.array(estimation_data['stock_return'], dtype=float)
            
            # 添加常数项
            X = sm.add_constant(X)
            
            # 进行回归
            model = sm.OLS(y, X).fit()
            return model.params[0], model.params[1]  # alpha, beta
            
        except Exception as e:
            logging.error(f"计算回归系数时发生错误: {str(e)}")
            raise
            
    def calculate_CAR(self, event_data: pd.DataFrame, alpha: float, beta: float, event_date: str, event_idx_in_window: int) -> pd.DataFrame:
        """
        计算累积异常收益率（CAR）
        
        Args:
            event_data: 事件窗口数据
            alpha: 回归截距
            beta: 回归系数
            event_date: 事件日期
            event_idx_in_window: 事件日在事件窗口中的索引
            
        Returns:
            包含异常收益率和累积异常收益率的数据
        """
        try:
            event_data = event_data.copy()
            event_data['predicted_return'] = event_data['index_return'] * beta + alpha
            event_data['abnormal_return'] = event_data['stock_return'] - event_data['predicted_return']
            event_data['cumulative_abnormal_return'] = event_data['abnormal_return'].cumsum()
            event_data['event_date'] = event_date
            
            # 添加事件日期的编号
            event_date_dt = pd.to_datetime(event_date)
            event_data['date_only'] = event_data['date'].dt.date
            event_date_only = event_date_dt.date()
            
            # 初始化relative_day列
            event_data['relative_day'] = 0
            
            # 设置相对日期：event_idx_in_window已经是0点（如果是非交易日，则是事件日之后最近的交易日）
            for i in range(len(event_data)):
                if i < event_idx_in_window:
                    event_data.iloc[i, event_data.columns.get_loc('relative_day')] = -(event_idx_in_window - i)
                elif i == event_idx_in_window:
                    event_data.iloc[i, event_data.columns.get_loc('relative_day')] = 0
                else:
                    event_data.iloc[i, event_data.columns.get_loc('relative_day')] = i - event_idx_in_window
            
            # 添加新列：交易日相对于事件日的位置
            event_data['trading_day_position'] = event_data['relative_day'].apply(
                lambda x: f"前第{abs(x)}个交易日" if x < 0 else ("事件日" if x == 0 else f"后第{x}个交易日")
            )
            logging.info(f"相对日期范围: {event_data['relative_day'].min()} 到 {event_data['relative_day'].max()}")

            return event_data
        except Exception as e:
            logging.error(f"计算CAR时发生错误: {str(e)}")
            raise
            
    def single_sample_test(self, car_data: pd.DataFrame, event_date: str) -> pd.DataFrame:
        """
        对累积异常收益率进行单样本t检验
        
        Args:
            car_data: 包含CAR的数据
            event_date: 事件日期
            
        Returns:
            检验结果的DataFrame
        """
        try:
            event_car = car_data[car_data['event_date'] == event_date]['cumulative_abnormal_return']
            t_stat, p_value = ttest_1samp(event_car, popmean=0)
            
            res = {
                'event_date': event_date,
                'mean_CAR': round(event_car.mean(), 4),
                't_stat': round(t_stat, 4),
                'p_value': round(p_value, 4),
                'significant': p_value < 0.05
            }
            
            return pd.DataFrame([res])
        except Exception as e:
            logging.error(f"进行t检验时发生错误: {str(e)}")
            raise

def process_event_wrapper(row_data: Dict[str, Any]) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    """
    处理单个事件的包装函数，用于多进程调用
    """
    try:
        company_code = row_data['company_code']
        event_date = row_data['event_date']
        
        # 获取当前进程信息（用于日志记录）
        current_process = multiprocessing.current_process()
        process_name = current_process.name
        
        # 在每个进程中创建独立的EventStudy实例
        event_study = EventStudy(
            event_window_before=3, #事件日前3个交易日
            event_window_after=5, #事件日后5个交易日
            estimation_window_length=120, #估计窗口长度
            estimation_window_gap=5 #估计窗口间隔5个交易日
        )
        logging.info(f"使用参数设置: event_window_before={event_study.event_window_before}, "
                    f"event_window_after={event_study.event_window_after}, "
                    f"estimation_window_length={event_study.estimation_window_length}, "
                    f"estimation_window_gap={event_study.estimation_window_gap}")
        
        # 获取数据
        merged_data = event_study.get_data(company_code, event_date, custom_index='000300') #使用custom_index参数指定市场指数代码（'000300'指的是沪深300），如果不指定，就使用每个股票代码对应的市场指数

        # print('事件窗口设定为：{} | 估计窗口设定为：{} | 大盘指数设定为：{} '.format([-(event_study.event_window_before), event_study.event_window_after], [-(event_study.estimation_window_gap+1+event_study.estimation_window_length),-(event_study.estimation_window_gap+1)], '沪深300'))


        if merged_data is None:
            logging.debug(f"[{process_name}] 跳过: 公司 {company_code} (数据获取失败)")
            return None, None
            
        # 提取事件数据
        estimation_data, event_window_data, event_idx_in_window = event_study.extract_event_data(event_date, merged_data)
        if estimation_data is None or event_window_data is None:
            logging.debug(f"[{process_name}] 跳过: 公司 {company_code} (事件数据提取失败)")
            return None, None
            
        # 计算回归系数
        alpha, beta = event_study.calculate_regression_coefficients(estimation_data)
        
        # 计算CAR
        car_result = event_study.calculate_CAR(event_window_data, alpha, beta, event_date, event_idx_in_window)
        
        # 添加公司相关信息
        car_result['company_code'] = company_code
        car_result['company_year'] = row_data['company_year']
        car_result['industry_code'] = row_data['industry_code']
        car_result['company_province'] = row_data['company_province']
        car_result['company_city'] = row_data['company_city']
        
    # 删除trading_day_position列
        car_result = car_result.drop(columns=['trading_day_position'])
        
        # 将date列改名为trading_date
        car_result = car_result.rename(columns={'date': 'trading_date'})
        
        # 按照指定顺序重新排列列
        desired_columns_order = [
            'company_code', 'company_year', 'trading_date', 'event_date', 'relative_day',
            'stock_return', 'index_return', 'date_only', 'predicted_return',
            'abnormal_return', 'cumulative_abnormal_return', '公司行业代码',
            '公司省份', '公司城市'
        ]
        
        # 确保所有需要的列都存在
        existing_columns = car_result.columns.tolist()
        columns_to_include = [col for col in desired_columns_order if col in existing_columns]
        
        # 重新排列列
        car_result = car_result[columns_to_include]
        
        # 删除包含NaN的行
        car_result = car_result.dropna()
        
        # 进行t检验
        t_test = event_study.single_sample_test(car_result, event_date)
        t_test['company_code'] = company_code
        t_test = t_test.dropna()
        t_test = t_test[['company_code','event_date','mean_CAR','t_stat','p_value','significant']] #重新排序
        
        logging.debug(f"[{process_name}] 完成: 公司 {company_code}, 日期 {event_date}")
        return car_result, t_test
        
    except Exception as e:
        logging.error(f"[{multiprocessing.current_process().name}] 处理公司 {row_data.get('company_code')} 时发生错误: {str(e)}")
        return None, None

def main():
    try:
        # 读取事件数据
        event_data = pd.read_csv('Event study_data/event info.csv')
        
        # 确保输出目录存在
        os.makedirs('Event study_results', exist_ok=True)
        
        car_csv_path = 'Event study_results/异常收益率（AR）&累积异常收益率(CAR).csv'
        t_test_csv_path = 'Event study_results/平均累计异常收益率（Mean CAR）_统计检验结果_按公司与事件日分类.csv'
        
        # 优化：预先读取已处理的数据，避免重复计算
        processed_events = set()
        if os.path.exists(car_csv_path):
            try:
                existing_data = pd.read_csv(car_csv_path)
                if not existing_data.empty and 'company_code' in existing_data.columns and 'event_date' in existing_data.columns:
                    # 创建(公司代码, 事件日期)的集合
                    processed_events = set(zip(existing_data['company_code'].astype(str), existing_data['event_date'].astype(str)))
                    logging.info(f"已发现 {len(processed_events)} 条处理过的记录")
            except Exception as e:
                logging.warning(f"读取现有结果文件失败: {e}")

        # 准备待处理的任务列表
        tasks = []
        for index, row in event_data.iterrows():
            company_code = str(row['company_code'])
            event_date = str(row['event_date'])
            
            # 检查是否已处理
            if (company_code, event_date) in processed_events:
                continue
                
            tasks.append(row.to_dict())
            
        logging.info(f"共有 {len(event_data)} 个事件，剩余 {len(tasks)} 个待处理")
        
        if not tasks:
            logging.info("所有事件均已处理完毕")
            return

        # 使用多进程处理
        # 获取CPU核心数，保留几个核心给系统由用户自己设置
        num_processes = max(1, multiprocessing.cpu_count() - 6) # 保留6个核心给系统，并行处理线程不宜过多
        logging.info(f"启动 {num_processes} 个进程进行并行计算...")
        
        all_results = pd.DataFrame()
        t_test_res = pd.DataFrame()
        
        # 统计信息
        success_count = 0
        fail_count = 0
        
        # 创建进程池
        with multiprocessing.Pool(processes=num_processes) as pool:
            # 使用tqdm显示进度条
            with tqdm(total=len(tasks), desc="处理事件", unit="个", 
                     bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]') as pbar:
                # 使用imap_unordered获取结果，这样可以实时处理完成的任务
                for i, (car_result, t_test) in enumerate(pool.imap_unordered(process_event_wrapper, tasks)):
                    if car_result is not None and t_test is not None:
                        success_count += 1
                        # 实时写入CSV
                        # 写入CAR结果
                        if not os.path.exists(car_csv_path):
                            car_result.to_csv(car_csv_path, index=False, encoding='utf-8-sig')
                        else:
                            car_result.to_csv(car_csv_path, mode='a', header=False, index=False, encoding='utf-8-sig')
                            
                        # 写入t检验结果
                        if not os.path.exists(t_test_csv_path):
                            t_test.to_csv(t_test_csv_path, index=False, encoding='utf-8-sig')
                        else:
                            t_test.to_csv(t_test_csv_path, mode='a', header=False, index=False, encoding='utf-8-sig')
                        
                        # 收集结果用于最后保存Excel（如果内存允许）
                        # all_results = pd.concat([all_results, car_result], ignore_index=True)
                        # t_test_res = pd.concat([t_test_res, t_test], ignore_index=True)
                    else:
                        fail_count += 1
                    
                    # 更新进度条，显示成功和失败数量
                    pbar.set_postfix({'成功': success_count, '失败': fail_count})
                    pbar.update(1)
                    
                    # 每100个任务记录一次详细日志
                    if (i + 1) % 100 == 0:
                        logging.info(f"已处理 {i + 1}/{len(tasks)} 个事件 (成功: {success_count}, 失败: {fail_count})")

        logging.info(f"所有任务处理完成！总计: {len(tasks)} 个事件，成功: {success_count} 个，失败: {fail_count} 个")
            
    except Exception as e:
        logging.error(f"程序执行过程中发生错误: {str(e)}")
        raise

if __name__ == '__main__':
    # Windows下使用multiprocessing必须在if __name__ == '__main__':下调用
    multiprocessing.freeze_support()
    main()