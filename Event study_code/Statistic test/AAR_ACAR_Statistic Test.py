import pandas as pd
import numpy as np
from scipy import stats
import os

# 读取数据
input_file = 'Event study_results/异常收益率（AR）&累积异常收益率(CAR).csv'
if not os.path.exists(input_file):
    print(f"错误：找不到文件 {input_file}")
    exit(1)

df = pd.read_csv(input_file)

# 按relative_day分组
grouped = df.groupby('relative_day')

# 使用列表收集结果，比逐行写入DataFrame更高效且不易出错
results_list = []

# 对每个relative_day计算统计量
for day, group in grouped:
    print(f"正在处理相对日期: {day}")
    
    # 统计量计算函数
    def calculate_stats(series, prefix):
        # 基本统计量
        mean_val = series.mean()
        median_val = series.median()
        min_val = series.min()
        max_val = series.max()
        
        # 正负比例
        positive_count = (series > 0).sum()
        negative_count = (series < 0).sum()
        total_count = len(series)
        positive_ratio = positive_count / total_count
        negative_ratio = negative_count / total_count
        
        # 检验
        # t检验 (检验均值是否为0)
        t_stat, p_value = stats.ttest_1samp(series, 0)
        
        # Wilcoxon符号秩检验 (检验中位数是否为0)
        # 注意：Wilcoxon检验假设分布是对称的
        try:
            w_stat, w_p_value = stats.wilcoxon(series - 0) # 减0是为了明确检验目标
        except ValueError:
            # 如果所有值都相同（例如都是0），Wilcoxon可能会报错
            w_stat, w_p_value = np.nan, np.nan
            
        # 二项式符号检验 (检验正负比例是否为0.5)
        binom_test_positive = stats.binomtest(positive_count, total_count, p=0.5)
        binom_test_negative = stats.binomtest(negative_count, total_count, p=0.5)
        
        return {
            f'{prefix}_Mean': mean_val,
            f'{prefix}_Median': median_val,
            f'{prefix}_Min': min_val,
            f'{prefix}_Max': max_val,
            f'{prefix}_t_stat': t_stat,
            f'{prefix}_p_value': p_value,
            f'{prefix}_w_stat': w_stat,
            f'{prefix}_w_p_value': w_p_value,
            f'{prefix}_Positive_Ratio': positive_ratio,
            f'{prefix}_Negative_Ratio': negative_ratio,
            f'{prefix}_Binom_p_value_Positive': binom_test_positive.pvalue,
            f'{prefix}_Binom_p_value_Negative': binom_test_negative.pvalue
        }

    # 计算AR的统计量
    ar_stats = calculate_stats(group['abnormal_return'], 'AR')
    
    # 计算CAR的统计量
    car_stats = calculate_stats(group['cumulative_abnormal_return'], 'CAR')
    
    # 合并结果
    row_result = {'relative_day': day}
    row_result.update(ar_stats)
    row_result.update(car_stats)
    
    results_list.append(row_result)

# 创建DataFrame
results = pd.DataFrame(results_list)

# 设置relative_day为索引并排序
results = results.set_index('relative_day').sort_index()

# 保存结果到Excel文件 (CSV)
output_file = 'Event study_results/异常收益率（AR）&累积异常收益率(CAR)_统计检验结果_按相对日期分类.csv'
results.to_csv(output_file, encoding='utf-8-sig')

# 打印结果
print(f"分析结果已保存到 {output_file}")
print("\n结果预览：")
print(results)